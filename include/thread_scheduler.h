#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <chrono>

/**
 * @brief ThreadScheduler：跨多个 Sink/Pipeline 统一管理线程“额度”与创建。
 *
 * - 用途：作为每个 SinkModule / PipelineModule 的构造参数传入；
 * - 职责：限制全局线程总数、提供“线程额度”申请/归还（线程创建由模块自己完成）。
 *
 * 说明：
 * - 该调度器不做“任务队列”，只做额度管理与线程创建包装；
 * - 每个模块自身实现扩缩容策略（何时需要新线程 / 何时退出空闲线程）。
 */
class ThreadScheduler {
public:
    struct Options {
        // 全局允许的最大 worker 线程数（跨所有模块总和）
        std::size_t max_total_threads;
        // 最小创建线程间隔（微秒）。用于抑制短时间内爆发式扩容。
        // 0 表示不限制。
        std::uint64_t min_spawn_interval_us;

        static std::size_t DefaultMaxTotalThreads() {
            auto hc = std::thread::hardware_concurrency();
            return hc == 0 ? 8u : static_cast<std::size_t>(hc);
        }

        explicit Options(std::size_t max_total_threads_ = DefaultMaxTotalThreads(),
                         std::uint64_t min_spawn_interval_us_ = 0)
            : max_total_threads(max_total_threads_),
              min_spawn_interval_us(min_spawn_interval_us_) {}
    };

    /**
     * @brief 调用者句柄：每个模块持有一个 Caller，使 min_spawn_interval_us 按调用者独立计时。
     * @note Caller 只负责“申请/归还全局线程额度”，不负责创建线程。
     */
    class Caller {
    public:
        Caller() : scheduler_(nullptr), last_spawn_us_(0) {}

        Caller(const Caller&) = delete;
        Caller& operator=(const Caller&) = delete;

        Caller(Caller&& other) noexcept
            : scheduler_(other.scheduler_),
              last_spawn_us_(other.last_spawn_us_) {
            other.scheduler_ = nullptr;
        }

        Caller& operator=(Caller&& other) noexcept {
            if (this == &other) return *this;
            scheduler_ = other.scheduler_;
            last_spawn_us_ = other.last_spawn_us_;
            other.scheduler_ = nullptr;
            return *this;
        }

        /**
         * @brief 尝试申请“创建一个线程”的全局额度。
         * @return 成功则调用者可自行创建线程；失败表示触发 per-caller 间隔限制或全局线程已达上限。
         */
        bool TryAcquire() {
            if (!scheduler_) return false;
            if (!PassSpawnIntervalGate_()) return false;
            return scheduler_->TryAcquire_();
        }

        /**
         * @brief 归还一个已申请的全局线程额度。
         */
        void Release() {
            if (!scheduler_) return;
            scheduler_->Release_();
        }

    private:
        friend class ThreadScheduler;

        explicit Caller(ThreadScheduler* scheduler)
            : scheduler_(scheduler), last_spawn_us_(0) {}

        static std::uint64_t NowUs_() {
            using clock = std::chrono::steady_clock;
            return static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    clock::now().time_since_epoch())
                    .count());
        }

        bool PassSpawnIntervalGate_() {
            if (scheduler_->opt_.min_spawn_interval_us == 0) return true;

            const auto now = NowUs_();
            if (last_spawn_us_ != 0 &&
                now - last_spawn_us_ < scheduler_->opt_.min_spawn_interval_us) {
                return false;
            }
            last_spawn_us_ = now;
            return true;
        }

        ThreadScheduler* scheduler_;
        std::uint64_t last_spawn_us_;
    };

    explicit ThreadScheduler(Options opt = Options())
        : opt_(opt), active_threads_(0) {}

    ThreadScheduler(const ThreadScheduler&) = delete;
    ThreadScheduler& operator=(const ThreadScheduler&) = delete;

    std::size_t GetMaxTotalThreads() const { return opt_.max_total_threads; }
    std::size_t GetActiveThreads() const { return active_threads_.load(); }

    /**
     * @brief 创建一个“调用者句柄”。每个模块应持有一个 Caller，用于独立计时的扩容节奏。
     */
    Caller MakeCaller() { return Caller(this); }

private:
    bool TryAcquire_() {
        while (true) {
            auto cur = active_threads_.load();
            if (cur >= opt_.max_total_threads) return false;
            if (active_threads_.compare_exchange_weak(cur, cur + 1)) return true;
        }
    }

    void Release_() { active_threads_.fetch_sub(1); }

    Options opt_;
    std::atomic<std::size_t> active_threads_;
};


