#include <module.h>

#include <chrono>
#include <iostream>
#include <thread>

// 一个最小可运行示例：
// CounterSource(int) -> DoublerPipeline(int->int) -> PrintSink(int)

class CounterSource final : public SourcePipeline<int> {
public:
    explicit CounterSource(int n)
        : n_(n) {
        producer_ = std::thread([this]() { Produce(); });
    }

    ~CounterSource() override {
        if (producer_.joinable()) producer_.join();
    }

    void Release(int&& /*data*/) override {
        // 这里的数据是值类型，不需要回收；但必须实现以满足接口契约。
    }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {
        // 最小示例：不使用内存配额
    }

private:
    void Produce() {
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

    void Release(int&& /*data*/) override {
        // 下游用完我们输出的数据后，会调用这个 Release
    }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {
        // 最小示例：不使用内存配额
    }

protected:
    void Process(int&& input) override {
        // 用完上游输入后，显式 Release（对应上游 outstanding 计数）
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
        Emit(input * 2);
    }
};

class PrintSink final : public SinkPipeline<int> {
public:
    PrintSink(ThreadScheduler& scheduler, SourcePipeline<int>* upstream)
        : SinkPipeline<int>(scheduler, upstream) {}

    double GetFactor() const override { return 1.0; }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {
        // 最小示例：不使用内存配额
    }

protected:
    void Process(int&& input) override {
        std::cout << "got: " << input << "\n";
        // 用完数据后，显式 Release 给上游
        if (auto* up = GetUpstream()) {
            up->Release(std::move(input));
        }
    }
};

int main() {
    ThreadScheduler scheduler(ThreadScheduler::Options(4));

    CounterSource source(10);

    DoublerPipeline doubler(scheduler, &source);

    PrintSink sink(scheduler, &doubler);

    // 只启动最后一个 Sink，并传入“整条链”的总内存预算（字节）
    // 它会按 factor 切分内存，先调用每个模块的 OnMemorySet()，再按上游->下游启动。
    sink.Start(/*total_memory_bytes=*/1024 * 1024);

    doubler.WaitForCompletion();
    sink.WaitForCompletion();

    return 0;
}


