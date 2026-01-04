#pragma once

#include <module.h>

#include "file_chunk.h"

#include <filesystem>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <memory>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <string>

#include <rapidjson/document.h>

struct JsonDecoderOptions {
    // 若非空且在 UNIX 上：用该文件做 mmap arena（建议指向 tmpfs，例如 /dev/shm/...）
    std::string mmap_file_path{};
};

class DecoderMemoryPool;

using DecoderRapidJsonAllocator = rapidjson::MemoryPoolAllocator<DecoderMemoryPool>;
using DecoderRapidJsonDocument = rapidjson::GenericDocument<rapidjson::UTF8<>, DecoderRapidJsonAllocator>;

// Shared aliases for downstream modules that keep the parsed program JSON in pooled memory
using DecodedProgramDocument = DecoderRapidJsonDocument;
using DecodedProgramAllocator = DecoderRapidJsonAllocator;
using DecodedProgramValue = DecodedProgramDocument::ValueType;

/**
 * @brief 解码后的 JSON 数据结构
 * 包含解析后的 JSON 文档和元数据
 */
struct DecodedJson {
    DecodedJson() = default;
    DecodedJson(DecodedJson &&) noexcept;
    DecodedJson &operator=(DecodedJson &&) noexcept;
    DecodedJson(const DecodedJson &) = delete;
    DecodedJson &operator=(const DecodedJson &) = delete;
    ~DecodedJson() = default;  // 不再自动释放，由 Release() 手动管理

    std::unique_ptr<DecoderRapidJsonDocument> document;        // JSON 文档（使用指针避免移动引发的问题）
    std::filesystem::path source_path;          // 源文件路径
    std::size_t line_number = 0;                // 1-based 行号（用于追溯到源 JSONL 文件）
    std::size_t line_length = 0;                // 行长度
    const char* raw_json = nullptr;             // 原始 JSON 数据指针
    std::size_t raw_json_length = 0;            // 原始 JSON 数据长度

private:
    friend class JsonDecoderModule;

    std::unique_ptr<DecoderRapidJsonAllocator> allocator_; // rapidjson allocator backed by external pool
    
    // 用于管理 FileChunk 的引用（不使用 shared_ptr）
    const char* chunk_data_ptr_;                // FileChunk 的 data 指针（作为唯一标识符）
    bool released_ = false;                     // 是否已释放
};

/**
 * @brief JSON 解码器模块
 * 
 * 从上游模块（FileManagerModule）接收文件块，解析其中的 JSON 行并输出。
 * 支持多线程处理，每个文件块中的每一行 JSON 都会被解析为独立的 DecodedJson。
 * 
 * 输入类型：FileChunk (文件内容块)
 * 输出类型：DecodedJson (解析后的 JSON 文档)
 */
class JsonDecoderModule : public Pipeline<FileChunk, DecodedJson> {
public:
    using Options = JsonDecoderOptions;

    /**
     * @brief 构造函数
     * @param upstream 上游模块指针（FileManagerModule）
     * @param opt 模块选项（mmap/allocator chunk size 等）
     */
    explicit JsonDecoderModule(ThreadScheduler& scheduler,
                               SourcePipeline<FileChunk>* upstream,
                               Options opt = Options{});

    ~JsonDecoderModule() override;

    // 该模块的内存倍率（memory_this / memory_prev）
    double GetFactor() const override { return 4.0; }

    void OnMemorySet(std::size_t memory_bytes) override;

    /**
     * @brief 释放解码后的 JSON（归还 FileChunk 引用）
     * @param data 解码后的 JSON
     */
    void Release(DecodedJson&& data) override;

    /**
     * @brief 获取统计信息
     */
    struct Statistics {
        std::uint64_t chunks = 0;           // 处理的块数
        std::uint64_t bytes = 0;            // 处理的字节数
        std::uint64_t decoded_lines = 0;    // 解码的行数
    };
    
    Statistics GetStats() const;

protected:
    /**
     * @brief 处理文件块：解析其中的 JSON 行并输出
     * @param chunk 文件块
     */
    void Process(FileChunk&& chunk) override;

private:
    /**
     * @brief 解析文件块中的一行 JSON
     * @param line_start 行起始位置
     * @param line_end 行结束位置
     * @param chunk_data_ptr FileChunk 的 data 指针（作为唯一标识符）
     * @param source_path 源文件路径
     */
    void ProcessLine(
        const char* line_start,
        const char* line_end,
        std::size_t line_number,
        const char* chunk_data_ptr,
        const std::filesystem::path& source_path,
        const char* chunk_end
    );

    /**
     * @brief FileChunk 引用计数结构
     */
    struct ChunkRef {
        FileChunk chunk;                        // FileChunk 对象
        std::size_t ref_count;                  // 引用计数
        
        ChunkRef(FileChunk&& c) : chunk(std::move(c)), ref_count(0) {}
    };

    /**
     * @brief 增加 FileChunk 的引用计数
     * @param data_ptr FileChunk 的 data 指针（作为唯一标识符）
     */
    void IncrementRefCount(const char* data_ptr);

    /**
     * @brief 减少 FileChunk 的引用计数，如果为 0 则释放
     * @param data_ptr FileChunk 的 data 指针（作为唯一标识符）
     */
    void DecrementRefCount(const char* data_ptr);

    /**
     * @brief 如果 FileChunk 没有被任何 DecodedJson 引用，则立即归还给上游
     * @param data_ptr FileChunk 的 data 指针
     */
    void ReleaseChunkIfUnused(const char* data_ptr);

    using MemoryWaitGuard = typename SinkPipeline<FileChunk>::ScopedMemoryRequestGuard;

    // FileChunk 引用计数管理（使用 unordered_map + lock）
    // 使用 data 指针作为键，因为每个内存池块的数据指针是唯一的
    mutable std::mutex ref_count_mutex_;
    std::unordered_map<const char*, std::unique_ptr<ChunkRef>> chunk_refs_;  // data 指针 -> ChunkRef

    // Decoder 内存池（可选）
    Options opt_;
    std::unique_ptr<DecoderMemoryPool> decoder_memory_pool_;
    std::size_t decoder_chunk_size_ = 64 * 1024;

    // 统计信息
    mutable std::mutex stats_mutex_;
    std::uint64_t total_chunks_ = 0;
    std::uint64_t total_bytes_ = 0;
    std::uint64_t decoded_lines_ = 0;
};

// DecoderMemoryPool 实现 Allocator 概念，用于作为 rapidjson::MemoryPoolAllocator 的 BaseAllocator。
// 它在预分配的内存区域上维护可变大小的块，支持动态切分与合并。
class DecoderMemoryPool {
public:
    static const bool kNeedFree = true;  // 实现 Allocator 概念

    DecoderMemoryPool();

    DecoderMemoryPool(std::size_t buffer_limit,
                      const std::string& mmap_file_path,
                      std::size_t chunk_size);
    ~DecoderMemoryPool();

    // Allocator 接口
    void* Malloc(std::size_t size);
    void Free(void* ptr);

    void SetWaitObserver(std::function<void(bool)> observer) { wait_observer_ = std::move(observer); }

private:
    static constexpr std::size_t Align(std::size_t size) {
        constexpr std::size_t alignment = 8;  // 与 RAPIDJSON_ALIGN 对齐
        return (size + alignment - 1) & ~(alignment - 1);
    }

    struct alignas(8) BlockHeader {
        std::size_t size;           // 包含头部在内的整个块大小
        bool free;
        BlockHeader* prev;          // 物理上前一个块
        BlockHeader* next;          // 物理上后一个块
        BlockHeader* prev_free;     // 空闲链表中的前一个
        BlockHeader* next_free;     // 空闲链表中的后一个
    };

    static constexpr std::size_t HeaderSize() {
        return Align(sizeof(BlockHeader));
    }

    static constexpr std::size_t MinBlockSize() {
        return HeaderSize() + Align(16);
    }

    BlockHeader* InitializeFirstBlock();
    BlockHeader* FindFreeBlock(std::size_t total_size);
    void SplitBlock(BlockHeader* block, std::size_t total_size);
    void InsertFreeBlock(BlockHeader* block);
    void RemoveFreeBlock(BlockHeader* block);
    BlockHeader* Coalesce(BlockHeader* block);
    bool PointerInArena(const void* ptr) const;

    std::size_t buffer_limit_ = 0;
    std::string mmap_file_path_;
    bool use_mmap_ = false;
    int arena_fd_ = -1;
    char* arena_data_ = nullptr;
    std::size_t arena_size_ = 0;
    std::vector<char> arena_storage_;
    std::size_t usable_arena_size_ = 0;

    std::size_t chunk_size_ = 0;  // 记录建议 chunk 大小（供统计用）

    std::mutex mutex_;
    std::condition_variable alloc_cv_;
    std::function<void(bool)> wait_observer_;
    BlockHeader* head_block_ = nullptr;      // 整个 arena 的起始块
    BlockHeader* free_list_head_ = nullptr;  // 空闲块链表
    bool fallback_mode_ = false;      // 当未配置内存池时，退化为标准分配
};

