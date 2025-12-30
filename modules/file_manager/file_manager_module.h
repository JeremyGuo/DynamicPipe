#pragma once

#include <module.h>

#include "file_chunk.h"

#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

/**
 * @brief FileManager 模块：从上游读取文件路径，读取文件内容并输出 FileChunk。
 *
 * 特性：
 * - 支持 arena 内存池（vector 或 mmap 文件）；
 * - consume_only=true 时：arena 不够用会退化到独立 storage，不阻塞等待；
 * - 可选 mmap_file_path：在 POSIX 平台用 mmap 文件作为 arena。
 */
struct FileManagerOptions {
    // 仅消费模式：arena 不够用时使用独立 storage（不阻塞等待内存池）
    bool consume_only = false;

    // 若非空且在 UNIX 上：用该文件做 mmap arena（建议指向 tmpfs）
    std::string mmap_file_path{};
};

class FileManagerModule final : public Pipeline<std::string, FileChunk> {
public:
    using Options = FileManagerOptions;

    explicit FileManagerModule(ThreadScheduler& scheduler,
                               SourcePipeline<std::string>* upstream,
                               Options opt = Options{});

    ~FileManagerModule() override;

    double GetFactor() const override { return 1.0; }

    // consume_only=true 表示不依赖预分配 arena（arena 不够用就独立分配），因此不参与 factor 链的内存分配
    bool HasFactor() const override { return !opt_.consume_only; }

    void OnMemorySet(std::size_t memory_bytes) override;

    void Release(FileChunk&& data) override;

    struct Statistics {
        std::size_t total_files = 0;
        std::size_t processed_files = 0;
    };

    Statistics GetStats() const;

protected:
    void Process(std::string&& file_path) override;

private:
    struct Block {
        std::size_t offset = 0;
        std::size_t size = 0;
    };

    void InitArena_(std::size_t arena_bytes);
    void DestroyArena_();

    std::optional<Block> AcquireBlock_(std::size_t size, bool allow_wait);
    void ReleaseBlock_(Block block);
    bool HasBlockFor_(std::size_t size) const;

    using FreeBlocksByOffset = std::map<std::size_t, Block>;
    using SizeIndex = std::multimap<std::size_t, FreeBlocksByOffset::iterator>;

    FreeBlocksByOffset::iterator InsertBlock_(Block block);
    void RemoveBlock_(FreeBlocksByOffset::iterator it);
    void EraseSizeIndexFor_(FreeBlocksByOffset::iterator it);

    FileChunk ReadFile_(const std::filesystem::path& file_path, std::size_t size_bytes);

private:
    Options opt_;

    // arena
    std::vector<char> arena_vec_;
    char* arena_data_ = nullptr;
    std::size_t arena_size_ = 0;
    bool use_mmap_ = false;
    int arena_fd_ = -1;

    // allocator
    mutable std::mutex buffer_mu_;
    std::condition_variable buffer_cv_;
    FreeBlocksByOffset free_blocks_;
    SizeIndex size_index_;

    // stats
    mutable std::mutex stats_mu_;
    std::size_t total_files_ = 0;
    std::size_t processed_files_ = 0;
};


