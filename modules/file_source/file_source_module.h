#pragma once

#include <module.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

/**
 * @brief FileSource 模块：从单个文件/目录或给定文件列表中收集文件路径，并按文件大小从大到小输出。
 *
 * 设计：
 * - 继承 SourcePipeline<std::string>，使用框架内置的 Emit/CloseOutput 队列机制；
 * - 在 StartAfterMemory() 中启动 producer 线程（确保先完成 OnMemorySet）；
 * - 输出的 string 是文件路径（UTF-8，取决于平台文件系统编码）。
 */
class FileSourceModule final : public SourcePipeline<std::string> {
public:
    struct Statistics {
        std::uint64_t total_files = 0;
        std::uint64_t files_emitted = 0;
    };

    explicit FileSourceModule(const std::string& path,
                              const std::vector<std::string>& exclude_paths = {});

    explicit FileSourceModule(const std::vector<std::string>& file_paths,
                              const std::vector<std::string>& exclude_paths = {});

    ~FileSourceModule() override;

    // FileSource 几乎不需要额外内存预算：不参与 factor 链分配，上游部分将收到 OnMemorySet(0)
    bool HasFactor() const override { return false; }

    void OnMemorySet(std::size_t /*memory_bytes*/) override {}

    void StartAfterMemory(const AutoScaleOptions& /*opt*/) override;

    void Release(std::string&& /*data*/) override {}

    Statistics GetStats() const;

private:
    void InitExclude_(const std::vector<std::string>& exclude_paths);
    void BuildFilesFromPath_(const std::string& path);
    void BuildFilesFromList_(const std::vector<std::string>& file_paths);

    void CollectFilesFromDirectory_(const std::filesystem::path& dir_path,
                                   std::vector<std::filesystem::path>& files);
    bool ShouldExclude_(const std::filesystem::path& file_path) const;
    std::uintmax_t GetFileSize_(const std::filesystem::path& file_path) const;

    void Produce_();

private:
    std::vector<std::filesystem::path> exclude_paths_;
    std::vector<std::filesystem::path> sorted_files_;

    mutable std::mutex mu_;
    std::uint64_t emitted_{0};

    std::atomic<bool> stop_{false};
    std::atomic<bool> producer_started_{false};
    std::thread producer_;
};



