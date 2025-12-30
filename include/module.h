#pragma once

#ifdef __cplusplus

#include "thread_scheduler.h"

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <chrono>
#include <utility>
#include <cstddef>
#include <vector>
#include <cmath>

// 前向声明
template <typename, typename> class Pipeline;
template <typename> class SinkPipeline;
template <typename> class SourcePipeline;

/**
 * @brief 自动扩缩容配置（对所有 Sink/Pipeline 通用）
 */
struct AutoScaleOptions {
    // 多久评估一次是否需要扩容（“能够继续 Get” -> 按间隔慢慢加线程）
    std::chrono::milliseconds scale_up_interval{1000};
};

/**
 * @brief 所有模块的“虚基类”：用于跨模板实例做类型擦除，并承载内存配置回调。
 *
 * 说明：
 * - C++ 不存在“静态虚函数”，因此用普通虚函数表达 GetFactor/OnMemorySet 的多态。
 * - 该基类需要被 Source/Sink 以 virtual 方式继承，避免 Pipeline（Sink+Source）出现重复基类子对象。
 */
class ModuleBase {
public:
    virtual ~ModuleBase() = default;

    /**
     * @brief 当框架为该模块计算出内存配额后调用（必须实现）。
     * @note 该回调会在模块真正启动（创建 worker / producer）之前触发。
     */
    virtual void OnMemorySet(std::size_t memory_bytes) = 0;

    /**
     * @brief 内存倍率：memory_this / memory_prev
     * @note Source 默认 1.0；Sink/Pipeline 应覆写该函数。
     */
    virtual double GetFactor() const { return 1.0; }

    /**
     * @brief 是否参与“内存比例链”的计算。
     *
     * 语义（用于 SinkPipeline::Start(total_memory_bytes) 的全链分配）：
     * - 若某个模块返回 false，则该模块（以及更上游的模块）视为“几乎不占用内存”，会收到 OnMemorySet(0)；
     * - 内存比例链会从“最后一个 HasFactor()==false 的模块之后”重新开始计算，并把 total_memory_bytes 分配给后续模块。
     *
     * @note 默认 true，表示参与比例计算。
     */
    virtual bool HasFactor() const { return true; }

    /**
     * @brief 访问上游模块（用于从最后一个 Sink 回溯出整条链）
     * @note Source 默认无上游，返回 nullptr。
     */
    virtual ModuleBase* GetUpstreamModuleBase() const { return nullptr; }

    /**
     * @brief 在内存已设置完成后启动模块（worker/producer）。
     * @note 默认不做任何事；Sink/Pipeline 会覆写以启动 scaler/worker。
     */
    virtual void StartAfterMemory(const AutoScaleOptions& /*opt*/) {}

    /**
     * @brief 只触发一次的内存设置入口（公开）。
     */
    void ApplyMemoryOnce(std::size_t memory_bytes) { SetMemoryAndNotifyOnce_(memory_bytes); }

    /**
     * @brief 标记模块已启动（只会成功一次，用于全链幂等启动）。
     * @return true 表示本次成功从“未启动”切换为“已启动”；false 表示已是启动状态。
     */
    bool TryMarkStarted() { return !started_.exchange(true); }

    bool IsStarted() const { return started_.load(); }

protected:
    void SetMemoryAndNotifyOnce_(std::size_t memory_bytes) {
        if (memory_set_.exchange(true)) {
            return;
        }
        memory_bytes_.store(memory_bytes);
        OnMemorySet(memory_bytes);
    }

    std::size_t GetMemoryBytes_() const { return memory_bytes_.load(); }

private:
    std::atomic<bool> started_{false};
    std::atomic<bool> memory_set_{false};
    std::atomic<std::size_t> memory_bytes_{0};
};

/**
 * @brief 源模块（Source Module）
 * 只有输出，没有输入（没有上游）
 * 作为上游模块接口基类（用于类型擦除）
 * 提供基于队列的默认实现，子类可以重写 Get() 提供自定义实现
 * @tparam Tout 输出数据类型
 */
template <typename Tout>
class SourcePipeline : public virtual ModuleBase {
public:
    SourcePipeline()
        : output_closed_(false) {}

    virtual ~SourcePipeline() {
        // 确保输出队列被关闭，避免下游阻塞
        {
            std::lock_guard<std::mutex> lock(out_mutex_);
            output_closed_ = true;
        }
        out_cv_.notify_all();
    }

    /**
     * @brief 获取数据 (消费者调用)
     * @return std::optional<Tout> 有数据则返回，无数据且模块结束返回 nullopt
     */
    virtual std::optional<Tout> Get() {
        std::unique_lock<std::mutex> lock(out_mutex_);

        // 等待条件：有数据，或者输出通道已关闭
        out_cv_.wait(lock, [this] {
            if (out_queue_.empty()) {
                return output_closed_;
            }
            return true;
        });

        // 如果队列空了，且被标记为"再无数据"，返回终止信号
        if (out_queue_.empty() && output_closed_) {
            return std::nullopt;
        }

        // 取出数据
        Tout data = std::move(out_queue_.front());
        out_queue_.pop();
        return data;
    }

    /**
     * @brief 获取数据（带超时）
     * @note 用于让下游 worker “可观测空闲”，从而实现自动缩容。
     * @return 有数据则返回；若超时或已结束且无数据则返回 nullopt
     */
    template <typename Rep, typename Period>
    std::optional<Tout> GetFor(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock<std::mutex> lock(out_mutex_);

        const bool ok = out_cv_.wait_for(lock, timeout, [this] {
            if (out_queue_.empty()) {
                return output_closed_;
            }
            return true;
        });

        if (!ok) {
            // timeout
            return std::nullopt;
        }

        if (out_queue_.empty() && output_closed_) {
            return std::nullopt;
        }

        Tout data = std::move(out_queue_.front());
        out_queue_.pop();
        return data;
    }

    /**
     * @brief 判断是否“彻底结束且无数据可取”
     * @note output_closed_ 为 true 时不会再有新数据 Emit 进来，因此该判断对下游是安全的。
     */
    bool IsDrained() const {
        std::lock_guard<std::mutex> lock(out_mutex_);
        return output_closed_ && out_queue_.empty();
    }

    /**
     * @brief 当前输出队列积压（排队长度）
     * @note 用于下游模块做扩缩容决策。对于自定义 Get()/Emit 机制的子类，该值可能不具备意义。
     */
    std::size_t GetQueuedCount() const {
        std::lock_guard<std::mutex> lock(out_mutex_);
        return out_queue_.size();
    }

    /**
     * @brief 释放/归还数据 (对外接口)
     * @param data 此前由 Get() 获取的数据
     * @note 由下游模块在用完数据后显式调用，用于内存回收或句柄释放
     */
    virtual void Release(Tout&& data) = 0;

    /**
     * @brief 尝试获取数据（非阻塞）
     * @return 若当前队列有数据则返回；否则返回 nullopt（可能是暂时无数据，也可能是已结束）
     */
    std::optional<Tout> TryGet() {
        std::lock_guard<std::mutex> lock(out_mutex_);
        if (out_queue_.empty()) {
            return std::nullopt;
        }
        Tout data = std::move(out_queue_.front());
        out_queue_.pop();
        return data;
    }

protected:
    /**
     * @brief 将数据放入输出队列（供子类使用）
     */
    void Emit(Tout result) {
        std::lock_guard<std::mutex> lock(out_mutex_);
        out_queue_.push(std::move(result));
        out_cv_.notify_one(); // 通知阻塞在 Get() 上的下游
    }

    /**
     * @brief 关闭输出通道（供子类调用）
     * @note 调用之前必须保证所有要下游处理的数据都已进入输出队列
     */
    void CloseOutput() {
        std::lock_guard<std::mutex> lock(out_mutex_);
        output_closed_ = true;
        out_cv_.notify_all(); // 唤醒所有还在等待的下游线程
    }

private:
    // 输出队列相关
    std::queue<Tout> out_queue_;
    mutable std::mutex out_mutex_;
    std::condition_variable out_cv_;
    bool output_closed_; // 标记：输出队列是否已永久关闭
};

/**
 * @brief 终端模块（Sink Module）
 * 只有输入，没有输出（只有上游）
 * @tparam Tin 输入数据类型
 */
template <typename Tin>
class SinkPipeline : public virtual ModuleBase {
public:
    // 兼容旧代码：仍支持 SinkPipeline<T>::AutoScaleOptions 写法
    using AutoScaleOptions = ::AutoScaleOptions;

    explicit SinkPipeline(ThreadScheduler& scheduler, SourcePipeline<Tin>* upstream)
        : scheduler_(&scheduler),
          caller_(scheduler.MakeCaller()),
          upstream_(upstream),
          should_stop_(false),
          thread_stats_{} {
        if (!upstream_) {
            throw std::invalid_argument("SinkPipeline requires a non-null upstream");
        }
    }

    virtual ~SinkPipeline() {
        // 析构时必须确保线程安全退出
        should_stop_ = true;
        if (scaler_.joinable())
            scaler_.join();
    }

    /**
     * @brief 启动模块（推荐新接口）
     * @param total_memory_bytes 整条链可用的总内存（字节）
     *
     * 规则：
     * - 设链条为：M0(Source) -> M1 -> ... -> Mn(this sink)
     * - 对于 i>=1：Mi::GetFactor() = memory_i / memory_{i-1}
     * - 将 total_memory_bytes 按上述比例分配到每个模块，先调用每个模块的 OnMemorySet(memory_i)，
     *   然后按“上游到下游”的顺序启动（StartAfterMemory）。
     */
    void Start(std::size_t total_memory_bytes, AutoScaleOptions opt = {}) {
        if (IsStarted()) throw std::logic_error("SinkPipeline::Start() can only be called once");
        if (!scheduler_) throw std::logic_error("SinkPipeline requires a ThreadScheduler");

        // 1) 回溯链条：this(sink) -> ... -> source
        std::vector<ModuleBase*> rev;
        rev.reserve(8);
        for (ModuleBase* cur = this; cur != nullptr; cur = cur->GetUpstreamModuleBase()) {
            rev.push_back(cur);
        }

        // 2) 翻转为 source -> ... -> sink
        std::vector<ModuleBase*> chain;
        chain.reserve(rev.size());
        for (auto it = rev.rbegin(); it != rev.rend(); ++it) {
            chain.push_back(*it);
        }

        std::vector<std::size_t> mem(chain.size(), 0);
        if (chain.empty()) {
            return;
        }

        // 3) 根据 HasFactor() 决定从哪里开始参与比例分配：
        //    - 上游（含 Source）默认可能 HasFactor()==false，这些模块会收到 OnMemorySet(0)
        //    - 从“最后一个 HasFactor()==false 的模块之后”开始，将 total_memory_bytes 按 factor 链分配
        std::size_t start_idx = 0;
        for (std::size_t i = 0; i < chain.size(); ++i) {
            if (!chain[i]->HasFactor()) {
                start_idx = i + 1;
            }
        }

        if (start_idx >= chain.size()) {
            // 整条链都不参与 factor 分配：所有模块内存为 0
        } else if (total_memory_bytes == 0) {
            // 预算为 0：参与者也全是 0（保持默认）
        } else {
            // suffix: chain[start_idx..end] 参与分配
            const std::size_t m = chain.size() - start_idx;
            std::vector<long double> prefix_prod(m, 1.0L);
            for (std::size_t j = 1; j < m; ++j) {
                const double f = chain[start_idx + j]->GetFactor();
                if (!(f > 0.0) || !std::isfinite(f)) {
                    throw std::invalid_argument("GetFactor() must be finite and > 0");
                }
                prefix_prod[j] = prefix_prod[j - 1] * static_cast<long double>(f);
            }

            long double denom = 0.0L;
            for (const auto& p : prefix_prod) denom += p;
            if (denom <= 0.0L) {
                // 兜底：把预算全给最后一个模块
                mem.back() = total_memory_bytes;
            } else {
                const long double base = static_cast<long double>(total_memory_bytes) / denom;
                std::size_t assigned = 0;
                for (std::size_t j = 0; j < m; ++j) {
                    const auto v = static_cast<std::size_t>(std::floor(base * prefix_prod[j]));
                    mem[start_idx + j] = v;
                    assigned += v;
                }
                if (assigned <= total_memory_bytes) {
                    mem.back() += (total_memory_bytes - assigned);
                } else {
                    // 极端精度/溢出保护：回退为全给最后一个
                    std::fill(mem.begin(), mem.end(), 0);
                    mem.back() = total_memory_bytes;
                }
            }
        }

        // 4) 先设置每个模块内存（只触发一次）
        for (std::size_t i = 0; i < chain.size(); ++i) {
            chain[i]->ApplyMemoryOnce(mem[i]);
        }

        // 5) 再按“上游到下游”启动（幂等）
        for (ModuleBase* m : chain) {
            if (m->TryMarkStarted()) {
                m->StartAfterMemory(opt);
            }
        }
    }

    /**
     * @brief 每个 Sink/Pipeline 必须实现：描述该模块相对上游的内存倍率（memory_this / memory_prev）
     */
    double GetFactor() const override = 0;

    /**
     * @brief 等待所有处理完成
     */
    void WaitForCompletion() {
        if (scaler_.joinable())
            scaler_.join(); // All worker threads should be detached in TrySpawnWorker_
    }

protected:
    /**
     * @brief 业务处理逻辑（子类必须实现）
     * @note Process 接管 input 的所有权，处理完成后需要自己释放数据：
     *       可调用 GetUpstream()->Release(std::move(input))
     */
    virtual void Process(Tin&& input) = 0;

    /**
     * @brief 所有 worker 完成时的回调（可选重写）
     */
    virtual void OnAllWorkersDone() {}

    /**
     * @brief 获取上游模块指针（供子类使用）
     */
    SourcePipeline<Tin>* GetUpstream() const {
        return upstream_;
    }

private:
    ModuleBase* GetUpstreamModuleBase() const override {
        return static_cast<ModuleBase*>(upstream_);
    }

    void StartAfterMemory(const AutoScaleOptions& opt) override {
        opt_ = opt;
        should_stop_ = false;
        scaler_ = std::thread(&SinkPipeline::ScaleLoop_, this);
    }

    // ================= 内部线程循环 =================

    void WorkerEntry_() noexcept {
        // 确保：无论正常退出/提前退出/异常退出，都能正确记账并归还 ThreadScheduler 额度。
        struct Cleanup {
            SinkPipeline* self;
            ~Cleanup() noexcept {
                {
                    std::unique_lock<std::mutex> lock(self->thread_stats_.mutex);
                    // 关键：把 ThreadScheduler 额度归还也放在同一把锁的临界区内，
                    // 这样 scaler 线程观测到 active_workers_==0（在同一把锁下）时，
                    // 能建立 happens-before，保证所有 worker 的 Release() 已经发生，
                    // 避免 TSAN 报告“栈对象已离开作用域但后台线程仍在访问”的数据竞争。
                    self->caller_.Release();
                    if (self->thread_stats_.active_workers_ > 0) {
                        self->thread_stats_.active_workers_--;
                    } else {
                        // 防御：理论上不应发生（避免 underflow 导致 scaler 永远等不到 0）
                        self->thread_stats_.active_workers_ = 0;
                    }
                }
            }
        } cleanup{this};

        try {
            while (true) {
                // 检查是否需要停止（在获取数据前检查，避免阻塞）
                if (should_stop_) break;

                // 从上游模块拉取数据
                std::optional<Tin> input_opt = upstream_->GetFor(opt_.scale_up_interval * 0.8);

                if (!input_opt.has_value()) {
                    // 区分：上游已结束 vs 只是暂时没数据（poll 超时）
                    if (upstream_->IsDrained()) break;

                    // Check if we need to exit（缩容：若期望值 < 当前活跃数，则该 worker 自行退出）
                    {
                        std::unique_lock<std::mutex> lock(thread_stats_.mutex);
                        if (thread_stats_.desired_workers_ < thread_stats_.active_workers_) {
                            break;
                        }
                        thread_stats_.num_timeouts_++;
                    }
                    continue;
                }

                // 处理数据（子类实现）
                // 注意：Process 需要自己处理数据的释放，不要在这里调用 Release
                Tin input = std::move(input_opt.value());
                try {
                    Process(std::move(input));
                } catch (...) {
                    // 业务异常：终止该 worker，避免异常跨线程边界导致 std::terminate，
                    // 也避免持续抛异常造成忙等/日志风暴。
                    break;
                }
            }
        } catch (...) {
            // 兜底：吞掉所有异常，确保线程函数不抛出（否则将 std::terminate）
        }
    }

    void ScaleLoop_() {
        while (true) {
            // 上游完全结束后：等待 worker 自然退出，然后结束 scaler
            if (upstream_->IsDrained() || should_stop_) {
                while (true) {
                    std::this_thread::sleep_for(opt_.scale_up_interval);
                    {
                        std::unique_lock<std::mutex> lock(thread_stats_.mutex);
                        if (thread_stats_.active_workers_ == 0) {
                            OnAllWorkersDone();
                            return;
                        }
                    }
                }
                break;
            }

            bool spawn_reserved = false;
            {
                std::unique_lock<std::mutex> lock(thread_stats_.mutex);

                // Check if we need to scale up
                const auto backlog_count = upstream_->GetQueuedCount();
                if (thread_stats_.num_timeouts_ == 0 &&
                    backlog_count > thread_stats_.active_workers_) {
                    // 关键：先记账（reserve），再创建线程，避免 worker 先退出导致 active_workers_ 下溢/变负。
                    thread_stats_.desired_workers_++;
                    thread_stats_.active_workers_++;
                    spawn_reserved = true;
                }

                // Check if we need to scale down（desired 下限为 0）
                if (thread_stats_.num_timeouts_ > 0 &&
                    thread_stats_.active_workers_ == thread_stats_.desired_workers_) {
                    if (thread_stats_.desired_workers_ > 0) {
                        thread_stats_.desired_workers_--;
                    }
                }

                thread_stats_.num_timeouts_ = 0; // reset timeout count
            }

            if (spawn_reserved) {
                const bool success = TrySpawnWorker_();
                if (!success) {
                    // 线程创建/额度申请失败：回滚 reserve
                    std::unique_lock<std::mutex> lock(thread_stats_.mutex);
                    if (thread_stats_.active_workers_ > 0) thread_stats_.active_workers_--;
                    if (thread_stats_.desired_workers_ > 0) thread_stats_.desired_workers_--;
                }
            }

            std::this_thread::sleep_for(opt_.scale_up_interval);
        }
    }

    bool TrySpawnWorker_() {
        if (!scheduler_ || should_stop_) return false;
        if (!caller_.TryAcquire()) return false;
        try {
            // 注意：thread_stats_ 的 reserve/回滚在 ScaleLoop_ 中完成；这里仅负责全局额度 + 创建线程。
            std::thread t([this]() {
                try {
                    WorkerEntry_();
                } catch (...) {
                    // 避免异常穿透线程边界导致 std::terminate
                }
            });
            t.detach();
        } catch (...) {
            caller_.Release();
            return false;
        }
        return true;
    }

protected:
    // 上游模块指针（使用基类指针进行类型擦除）
    ThreadScheduler* scheduler_;
    ThreadScheduler::Caller caller_;
    SourcePipeline<Tin>* upstream_;

    // 线程管理
    std::thread scaler_;
    std::atomic<bool> should_stop_;
    AutoScaleOptions opt_;

    struct {
        std::size_t active_workers_; // 当前的 worker 数
        std::size_t desired_workers_; // 当前期望的 worker 数量

        // 当前检查区间是否发生过timeout_，如果有，说明worker足够多，不能创建新线程
        std::size_t num_timeouts_;

        std::mutex mutex;
    } thread_stats_;
};

/**
 * @brief 中间模块（Pipeline Module）
 * 多重继承 SinkPipeline（输入）和 SourcePipeline（输出）
 * 既有输入也有输出
 * @tparam Tin  输入数据类型
 * @tparam Tout 输出数据类型
 */
template <typename Tin, typename Tout>
class Pipeline : public SinkPipeline<Tin>, public SourcePipeline<Tout> {
public:
    /**
     * @brief 构造函数
     * @param upstream 上游模块指针（必须非空）
     */
    explicit Pipeline(ThreadScheduler& scheduler, SourcePipeline<Tin>* upstream)
        : SinkPipeline<Tin>(scheduler, upstream) {}

    /**
     * @brief 释放/归还数据 (对外接口)
     * @param data 此前由 Get() 获取的数据
     * @note 由下游模块在用完数据后显式调用，用于内存回收或句柄释放
     */
    void Release(Tout&& data) override = 0;

protected:
    /**
     * @brief 业务处理逻辑
     * @note 处理完成后，需调用 Emit(result) 输出
     *       如果需要释放上游数据，可以调用 GetUpstream()->Release(std::move(input))
     */
    void Process(Tin&& input) override = 0;

    /**
     * @brief 所有 worker 完成时的回调（可选重写）
     */
    void OnAllWorkersDone() override {
        // 当所有 worker 完成时，关闭输出通道
        SourcePipeline<Tout>::CloseOutput();
    }
};

#endif // __cplusplus


