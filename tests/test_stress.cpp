#include <module.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>

namespace {

[[noreturn]] void Fail(const char* expr, const char* file, int line, const std::string& msg) {
    std::cerr << "[STRESS FAIL] " << file << ":" << line << "  (" << expr << ")  " << msg << "\n";
    std::terminate();
}

#define REQUIRE_MSG(expr, msg) \
    do {                       \
        if (!(expr)) {         \
            Fail(#expr, __FILE__, __LINE__, (msg)); \
        }                      \
    } while (0)

#define REQUIRE(expr) REQUIRE_MSG((expr), "")

class CounterSource final : public SourcePipeline<std::uint64_t> {
public:
    explicit CounterSource(std::uint64_t n)
        : n_(n) {
        producer_ = std::thread([this]() { Produce_(); });
    }

    ~CounterSource() override {
        if (producer_.joinable()) producer_.join();
    }

    void Release(std::uint64_t&& /*data*/) override {}

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

private:
    void Produce_() {
        for (std::uint64_t i = 0; i < n_; ++i) {
            Emit(i);
        }
        CloseOutput();
    }

    std::uint64_t n_;
    std::thread producer_;
};

// y = a*x + b (mod 2^64)，用于构造多级流水线
class AffinePipeline final : public Pipeline<std::uint64_t, std::uint64_t> {
public:
    AffinePipeline(ThreadScheduler& scheduler, SourcePipeline<std::uint64_t>* upstream,
                   std::uint64_t a, std::uint64_t b)
        : Pipeline<std::uint64_t, std::uint64_t>(scheduler, upstream), a_(a), b_(b) {}

    double GetFactor() const override { return 1.0; }

    void Release(std::uint64_t&& /*data*/) override {}

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

protected:
    void Process(std::uint64_t&& input) override {
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
        Emit(a_ * input + b_);
    }

private:
    std::uint64_t a_;
    std::uint64_t b_;
};

class StatsSink final : public SinkPipeline<std::uint64_t> {
public:
    StatsSink(ThreadScheduler& scheduler, SourcePipeline<std::uint64_t>* upstream,
              std::atomic<std::uint64_t>* count,
              std::atomic<std::uint64_t>* sum,
              std::atomic<std::uint64_t>* xorsum)
        : SinkPipeline<std::uint64_t>(scheduler, upstream),
          count_(count),
          sum_(sum),
          xorsum_(xorsum) {
        if (!count_ || !sum_ || !xorsum_) throw std::invalid_argument("StatsSink requires non-null stats");
    }

    double GetFactor() const override { return 1.0; }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

protected:
    void Process(std::uint64_t&& input) override {
        count_->fetch_add(1, std::memory_order_relaxed);
        sum_->fetch_add(input, std::memory_order_relaxed);
        xorsum_->fetch_xor(input, std::memory_order_relaxed);
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
    }

private:
    std::atomic<std::uint64_t>* count_;
    std::atomic<std::uint64_t>* sum_;
    std::atomic<std::uint64_t>* xorsum_;
};

struct ExpectedStats {
    std::uint64_t count{};
    std::uint64_t sum{};
    std::uint64_t xorsum{};
};

ExpectedStats ComputeExpected(std::uint64_t n, std::uint64_t a1, std::uint64_t b1,
                              std::uint64_t a2, std::uint64_t b2,
                              std::uint64_t a3, std::uint64_t b3) {
    ExpectedStats e;
    e.count = n;
    std::uint64_t s = 0;
    std::uint64_t x = 0;
    for (std::uint64_t i = 0; i < n; ++i) {
        std::uint64_t y = i;
        y = a1 * y + b1;
        y = a2 * y + b2;
        y = a3 * y + b3;
        s += y;
        x ^= y;
    }
    e.sum = s;
    e.xorsum = x;
    return e;
}

void RunOneCase(std::mt19937_64& rng, int case_id) {
    std::uniform_int_distribution<int> threads_dist(2, 16);
    std::uniform_int_distribution<int> n_dist(1, 20000);
    std::uniform_int_distribution<int> ms_dist(1, 4);
    std::uniform_int_distribution<std::uint64_t> coeff_dist(1, 17); // 小系数，便于期望计算

    const int max_threads = threads_dist(rng);
    const std::uint64_t n = static_cast<std::uint64_t>(n_dist(rng));
    const auto interval = std::chrono::milliseconds(ms_dist(rng));

    const std::uint64_t a1 = coeff_dist(rng), b1 = coeff_dist(rng);
    const std::uint64_t a2 = coeff_dist(rng), b2 = coeff_dist(rng);
    const std::uint64_t a3 = coeff_dist(rng), b3 = coeff_dist(rng);

    ThreadScheduler scheduler(ThreadScheduler::Options(static_cast<std::size_t>(max_threads)));

    CounterSource source(n);
    AffinePipeline p1(scheduler, &source, a1, b1);
    AffinePipeline p2(scheduler, &p1, a2, b2);
    AffinePipeline p3(scheduler, &p2, a3, b3);

    std::atomic<std::uint64_t> got_count{0};
    std::atomic<std::uint64_t> got_sum{0};
    std::atomic<std::uint64_t> got_xor{0};
    StatsSink sink(scheduler, &p3, &got_count, &got_sum, &got_xor);

    SinkPipeline<std::uint64_t>::AutoScaleOptions opt;
    opt.scale_up_interval = interval;

    // 只启动最后一个 sink：内部会先启动上游再启动下游
    sink.Start(/*total_memory_bytes=*/0, opt);

    p1.WaitForCompletion();
    p2.WaitForCompletion();
    p3.WaitForCompletion();
    sink.WaitForCompletion();

    const auto expected = ComputeExpected(n, a1, b1, a2, b2, a3, b3);

    REQUIRE_MSG(got_count.load() == expected.count, "count mismatch in case " + std::to_string(case_id));
    REQUIRE_MSG(got_sum.load() == expected.sum, "sum mismatch in case " + std::to_string(case_id));
    REQUIRE_MSG(got_xor.load() == expected.xorsum, "xor mismatch in case " + std::to_string(case_id));
    REQUIRE_MSG(scheduler.GetActiveThreads() == 0, "ThreadScheduler leak in case " + std::to_string(case_id));
}

} // namespace

int main() {
    try {
        // 固定 seed：可复现。你也可以改成用时间种子做“更野”的 fuzz。
        std::mt19937_64 rng(0xC0FFEEULL);

        // 轮数控制在“能稳定抓 race / 卡死”的同时，保持 CI 快。
        constexpr int kCases = 50;
        for (int i = 0; i < kCases; ++i) {
            RunOneCase(rng, i);
        }
    } catch (const std::exception& e) {
        std::cerr << "[STRESS FAIL] uncaught std::exception: " << e.what() << "\n";
        return 2;
    } catch (...) {
        std::cerr << "[STRESS FAIL] uncaught unknown exception\n";
        return 3;
    }

    std::cout << "[STRESS PASS] ok\n";
    return 0;
}


