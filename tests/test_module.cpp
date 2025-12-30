#include <module.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

[[noreturn]] void Fail(const char* expr, const char* file, int line, const std::string& msg) {
    std::cerr << "[TEST FAIL] " << file << ":" << line << "  (" << expr << ")  " << msg << "\n";
    std::terminate();
}

#define REQUIRE_MSG(expr, msg) \
    do {                       \
        if (!(expr)) {         \
            Fail(#expr, __FILE__, __LINE__, (msg)); \
        }                      \
    } while (0)

#define REQUIRE(expr) REQUIRE_MSG((expr), "")

class CounterSource final : public SourcePipeline<int> {
public:
    explicit CounterSource(int n)
        : n_(n) {
        producer_ = std::thread([this]() { Produce_(); });
    }

    ~CounterSource() override {
        if (producer_.joinable()) producer_.join();
    }

    void Release(int&& /*data*/) override {}

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

private:
    void Produce_() {
        for (int i = 0; i < n_; ++i) {
            Emit(i);
        }
        CloseOutput();
    }

    int n_;
    std::thread producer_;
};

class DoublerPipeline final : public Pipeline<int, int> {
public:
    DoublerPipeline(ThreadScheduler& scheduler, SourcePipeline<int>* upstream)
        : Pipeline<int, int>(scheduler, upstream) {}

    double GetFactor() const override { return 1.0; }

    void Release(int&& /*data*/) override {}

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

protected:
    void Process(int&& input) override {
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
        Emit(input * 2);
    }
};

class CollectSink final : public SinkPipeline<int> {
public:
    CollectSink(ThreadScheduler& scheduler, SourcePipeline<int>* upstream, std::vector<int>* out)
        : SinkPipeline<int>(scheduler, upstream), out_(out) {
        if (!out_) throw std::invalid_argument("CollectSink requires non-null out");
    }

    double GetFactor() const override { return 1.0; }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

protected:
    void Process(int&& input) override {
        {
            std::lock_guard<std::mutex> lock(mu_);
            out_->push_back(input);
        }
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
    }

private:
    std::mutex mu_;
    std::vector<int>* out_;
};

void Test_EndToEnd_DataCorrect() {
    ThreadScheduler scheduler(ThreadScheduler::Options(/*max_total_threads=*/8));

    constexpr int N = 1000;
    CounterSource source(N);
    DoublerPipeline doubler(scheduler, &source);

    std::vector<int> got;
    got.reserve(N);
    CollectSink sink(scheduler, &doubler, &got);

    SinkPipeline<int>::AutoScaleOptions opt;
    // 单元测试希望尽快结束：把扩缩容评估间隔调小，避免 WaitForCompletion 里额外 sleep 造成“秒级”耗时。
    opt.scale_up_interval = std::chrono::milliseconds(2);

    // 只启动最后一个 sink：它会按 factor 分配“整条链”的总内存（这里用 0，表示各模块拿到 0）
    // 并按上游->下游顺序启动所有模块。
    sink.Start(/*total_memory_bytes=*/0, opt);

    doubler.WaitForCompletion();
    sink.WaitForCompletion();

    // 核验：数量正确（每个输入对应一个输出）
    REQUIRE_MSG(static_cast<int>(got.size()) == N, "output size mismatch");

    // 并发下顺序不保证：排序后对比集合
    std::sort(got.begin(), got.end());
    for (int i = 0; i < N; ++i) {
        REQUIRE_MSG(got[i] == i * 2, "unexpected output value");
    }

    // 所有 worker 退出后，全局额度应归零
    REQUIRE_MSG(scheduler.GetActiveThreads() == 0, "ThreadScheduler active threads should be 0 after completion");
}

void Test_StartOnlyOnce_Throws() {
    ThreadScheduler scheduler(ThreadScheduler::Options(/*max_total_threads=*/2));
    CounterSource source(1);
    DoublerPipeline doubler(scheduler, &source);

    std::vector<int> got;
    CollectSink sink(scheduler, &doubler, &got);

    SinkPipeline<int>::AutoScaleOptions opt;
    opt.scale_up_interval = std::chrono::milliseconds(2);

    // 只启动最后一个 sink：内部会先启动上游再启动下游。
    sink.Start(/*total_memory_bytes=*/0, opt);
    bool threw = false;
    try {
        sink.Start(/*total_memory_bytes=*/0, opt);
    } catch (const std::logic_error&) {
        threw = true;
    }

    // 保证正常收尾
    doubler.WaitForCompletion();
    sink.WaitForCompletion();

    REQUIRE_MSG(threw, "Start() twice should throw std::logic_error");
}

} // namespace

int main() {
    try {
        Test_EndToEnd_DataCorrect();
        Test_StartOnlyOnce_Throws();
    } catch (const std::exception& e) {
        std::cerr << "[TEST FAIL] uncaught std::exception: " << e.what() << "\n";
        return 2;
    } catch (...) {
        std::cerr << "[TEST FAIL] uncaught unknown exception\n";
        return 3;
    }

    std::cout << "[TEST PASS] all tests passed\n";
    return 0;
}


