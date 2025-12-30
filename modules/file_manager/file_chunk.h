#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <vector>

/**
 * @brief 文件内容块：要么指向 FileManager 的内存池（arena），要么持有独立分配的 storage。
 *
 * 约定：
 * - 当 storage != nullptr：data 指向 storage->data()，释放时无需归还到内存池；
 * - 当 storage == nullptr 且 data != nullptr：data 指向 FileManager 的 arena，释放时必须调用下游->Release() 归还；
 * - 当 size==0：data 允许为 nullptr。
 */
struct FileChunk {
    char* data = nullptr;
    std::size_t size = 0;
    std::filesystem::path path;
    std::unique_ptr<std::vector<char>> storage; // 独立存储（move-only）

    FileChunk() = default;

    FileChunk(FileChunk&& other) noexcept
        : data(other.data),
          size(other.size),
          path(std::move(other.path)),
          storage(std::move(other.storage)) {
        other.data = nullptr;
        other.size = 0;
    }

    FileChunk& operator=(FileChunk&& other) noexcept {
        if (this != &other) {
            data = other.data;
            size = other.size;
            path = std::move(other.path);
            storage = std::move(other.storage);
            other.data = nullptr;
            other.size = 0;
        }
        return *this;
    }

    FileChunk(const FileChunk&) = delete;
    FileChunk& operator=(const FileChunk&) = delete;
};



