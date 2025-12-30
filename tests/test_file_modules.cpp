#include <module.h>

#include <file_manager_module.h>
#include <file_source_module.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
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

namespace fs = std::filesystem;

static void WriteFile(const fs::path& p, const std::string& s) {
    fs::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary);
    REQUIRE_MSG(out.is_open(), "cannot open file for write: " + p.string());
    out.write(s.data(), static_cast<std::streamsize>(s.size()));
    REQUIRE_MSG(out.good(), "write failed: " + p.string());
}

class CollectSink final : public SinkPipeline<FileChunk> {
public:
    CollectSink(ThreadScheduler& scheduler,
                SourcePipeline<FileChunk>* upstream,
                std::unordered_map<std::string, std::string>* expected,
                std::vector<std::pair<std::string, std::size_t>>* out)
        : SinkPipeline<FileChunk>(scheduler, upstream),
          expected_(expected),
          out_(out) {
        if (!expected_ || !out_) throw std::invalid_argument("CollectSink requires non-null pointers");
    }

    double GetFactor() const override { return 1.0; }
    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

protected:
    void Process(FileChunk&& chunk) override {
        const std::string filename = chunk.path.filename().string();
        const std::size_t sz = chunk.size;

        // 校验内容（非空文件）
        auto it = expected_->find(filename);
        REQUIRE_MSG(it != expected_->end(), "unexpected file: " + filename);
        const std::string& exp = it->second;
        REQUIRE_MSG(exp.size() == sz, "size mismatch for: " + filename);
        if (sz > 0) {
            REQUIRE_MSG(chunk.data != nullptr, "chunk.data is null for non-empty file: " + filename);
            const std::string got(chunk.data, chunk.data + sz);
            REQUIRE_MSG(got == exp, "content mismatch for: " + filename);
        }

        {
            std::lock_guard<std::mutex> lock(mu_);
            out_->push_back({filename, sz});
        }

        if (auto* up = GetUpstream()) {
            up->Release(std::move(chunk));
        }
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::string>* expected_;
    std::vector<std::pair<std::string, std::size_t>>* out_;
};

static fs::path MakeTempDir() {
    const auto base = fs::temp_directory_path();
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    fs::path dir = base / ("modulardp_file_modules_" + std::to_string(static_cast<long long>(now)));
    fs::create_directories(dir);
    return dir;
}

static void RunEndToEnd(bool consume_only) {
    fs::path dir = MakeTempDir();

    // 构造 3 个文件，大小分别为 1 / 10 / 3
    WriteFile(dir / "a.txt", "a");
    WriteFile(dir / "b.txt", "0123456789");
    WriteFile(dir / "sub" / "c.txt", "ccc");

    std::unordered_map<std::string, std::string> expected{
        {"a.txt", "a"},
        {"b.txt", "0123456789"},
        {"c.txt", "ccc"},
    };

    ThreadScheduler scheduler(ThreadScheduler::Options(/*max_total_threads=*/4));

    FileSourceModule source(dir.string());

    FileManagerModule::Options fm_opt;
    fm_opt.consume_only = consume_only;

    FileManagerModule manager(scheduler, &source, fm_opt);

    std::vector<std::pair<std::string, std::size_t>> got;
    got.reserve(3);
    CollectSink sink(scheduler, &manager, &expected, &got);

    SinkPipeline<FileChunk>::AutoScaleOptions opt;
    opt.scale_up_interval = std::chrono::milliseconds(2);

    sink.Start(/*total_memory_bytes=*/1024 * 1024, opt);

    manager.WaitForCompletion();
    sink.WaitForCompletion();

    // 结果集合正确（并发下顺序不保证）
    REQUIRE_MSG(got.size() == 3, "expected 3 files");
    std::sort(got.begin(), got.end());
    REQUIRE_MSG(got[0].first == "a.txt" && got[0].second == 1, "unexpected a.txt");
    REQUIRE_MSG(got[1].first == "b.txt" && got[1].second == 10, "unexpected b.txt");
    REQUIRE_MSG(got[2].first == "c.txt" && got[2].second == 3, "unexpected c.txt");

    // Source 统计
    const auto st = source.GetStats();
    REQUIRE_MSG(st.total_files == 3, "source total_files mismatch");
    REQUIRE_MSG(st.files_emitted == 3, "source emitted mismatch");

    // 清理
    std::error_code ec;
    fs::remove_all(dir, ec);
}

void Test_FileSource_FileManager_EndToEnd() {
    RunEndToEnd(/*consume_only=*/true);
    RunEndToEnd(/*consume_only=*/false);
}

} // namespace

int main() {
    try {
        Test_FileSource_FileManager_EndToEnd();
    } catch (const std::exception& e) {
        std::cerr << "[TEST FAIL] uncaught std::exception: " << e.what() << "\n";
        return 2;
    } catch (...) {
        std::cerr << "[TEST FAIL] uncaught unknown exception\n";
        return 3;
    }
    std::cout << "[TEST PASS] file modules tests passed\n";
    return 0;
}



