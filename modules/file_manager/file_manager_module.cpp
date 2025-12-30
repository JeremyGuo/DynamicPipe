#include "file_manager_module.h"

#include <cmath>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

FileManagerModule::FileManagerModule(ThreadScheduler& scheduler,
                                     SourcePipeline<std::string>* upstream,
                                     Options opt)
    : Pipeline<std::string, FileChunk>(scheduler, upstream),
      opt_(std::move(opt)) {
}

FileManagerModule::~FileManagerModule() {
    DestroyArena_();
}

void FileManagerModule::OnMemorySet(std::size_t memory_bytes) {
    // 约定：
    // - consume_only=true：不预分配 arena，读文件时直接独立分配（malloc/new 语义）。
    // - consume_only=false：必须使用 arena（vector 或 mmap），读文件时只从 arena 分配，拿不到则阻塞等待。
    if (opt_.consume_only) {
        InitArena_(0);
        return;
    }

    // 非消费模式：arena 大小由框架内存配额决定
    InitArena_(memory_bytes);
}

void FileManagerModule::Process(std::string&& file_path) {
    // 无论如何都要把上游 string “归还”（满足接口契约；上游可能为空实现）
    struct UpstreamReleaseGuard {
        SourcePipeline<std::string>* up = nullptr;
        std::string* s = nullptr;
        ~UpstreamReleaseGuard() {
            if (up && s) up->Release(std::move(*s));
        }
    } guard{GetUpstream(), &file_path};

    fs::path path(file_path);

    std::error_code ec;
    if (!fs::exists(path, ec) || ec) return;
    if (!fs::is_regular_file(path, ec) || ec) return;

    const auto file_size = fs::file_size(path, ec);
    if (ec) {
        throw std::runtime_error("FileManagerModule: failed to stat file: " + path.string());
    }

    {
        std::lock_guard<std::mutex> lock(stats_mu_);
        total_files_++;
    }

    // 空文件也要输出一个 chunk（size=0），方便下游知道该文件存在
    if (file_size == 0) {
        FileChunk chunk;
        chunk.data = nullptr;
        chunk.size = 0;
        chunk.path = path;
        Emit(std::move(chunk));
        return;
    }

    const std::size_t sz = static_cast<std::size_t>(file_size);
    if (!opt_.consume_only && arena_size_ > 0 && sz > arena_size_) {
        // 非消费模式：要求能放进 arena（否则本模块的内存预算与语义不匹配）
        throw std::runtime_error("FileManagerModule: file exceeds arena size: " + path.string());
    }

    FileChunk chunk = ReadFile_(path, sz);

    {
        std::lock_guard<std::mutex> lock(stats_mu_);
        processed_files_++;
    }

    Emit(std::move(chunk));
}

void FileManagerModule::Release(FileChunk&& data) {
    if (data.size == 0 || data.data == nullptr) {
        return;
    }

    // 独立存储：让 unique_ptr 自己析构即可
    if (data.storage) {
        return;
    }

    // arena 释放
    const char* base = arena_data_;
    const char* end = base + arena_size_;
    if (!base || arena_size_ == 0) {
        throw std::logic_error("FileManagerModule: releasing arena chunk but arena is not initialized");
    }

    if (data.data < base || data.data + data.size > end) {
        throw std::out_of_range("FileManagerModule: released chunk does not belong to arena");
    }

    const std::size_t offset = static_cast<std::size_t>(data.data - base);
    ReleaseBlock_(Block{offset, data.size});
}

FileManagerModule::Statistics FileManagerModule::GetStats() const {
    Statistics s;
    std::lock_guard<std::mutex> lock(stats_mu_);
    s.total_files = total_files_;
    s.processed_files = processed_files_;
    return s;
}

void FileManagerModule::InitArena_(std::size_t arena_bytes) {
    DestroyArena_();

    arena_size_ = arena_bytes;
    if (arena_bytes == 0) {
        arena_data_ = nullptr;
        return;
    }

    use_mmap_ = !opt_.mmap_file_path.empty();

#if defined(__unix__) || defined(__APPLE__)
    if (use_mmap_) {
        arena_fd_ = ::open(opt_.mmap_file_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (arena_fd_ < 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "FileManagerModule: open mmap file failed: " + opt_.mmap_file_path);
        }
        if (::ftruncate(arena_fd_, static_cast<off_t>(arena_size_)) != 0) {
            const int err = errno;
            ::close(arena_fd_);
            arena_fd_ = -1;
            throw std::system_error(err, std::generic_category(),
                                    "FileManagerModule: ftruncate failed: " + opt_.mmap_file_path);
        }

        void* p = ::mmap(nullptr, arena_size_, PROT_READ | PROT_WRITE, MAP_SHARED, arena_fd_, 0);
        if (p == MAP_FAILED) {
            const int err = errno;
            ::close(arena_fd_);
            arena_fd_ = -1;
            throw std::system_error(err, std::generic_category(),
                                    "FileManagerModule: mmap failed: " + opt_.mmap_file_path);
        }

        arena_data_ = static_cast<char*>(p);
    } else {
        arena_vec_.resize(arena_size_);
        arena_data_ = arena_vec_.data();
    }
#else
    // 非 POSIX：不支持 mmap，退化为 vector
    use_mmap_ = false;
    arena_vec_.resize(arena_size_);
    arena_data_ = arena_vec_.data();
#endif

    // init allocator freelist
    {
        std::lock_guard<std::mutex> lock(buffer_mu_);
        free_blocks_.clear();
        size_index_.clear();
        InsertBlock_(Block{0, arena_size_});
    }
}

void FileManagerModule::DestroyArena_() {
#if defined(__unix__) || defined(__APPLE__)
    if (use_mmap_ && arena_data_ && arena_size_ > 0) {
        ::munmap(arena_data_, arena_size_);
    }
    if (arena_fd_ >= 0) {
        ::close(arena_fd_);
    }
#endif
    arena_fd_ = -1;
    use_mmap_ = false;
    arena_data_ = nullptr;
    arena_size_ = 0;
    arena_vec_.clear();
    arena_vec_.shrink_to_fit();

    std::lock_guard<std::mutex> lock(buffer_mu_);
    free_blocks_.clear();
    size_index_.clear();
}

FileChunk FileManagerModule::ReadFile_(const fs::path& file_path, std::size_t size_bytes) {
    Block block{};
    bool uses_arena = false;
    std::unique_ptr<std::vector<char>> storage;

    if (opt_.consume_only) {
        // 消费模式：总是独立分配，不依赖 arena
        storage = std::make_unique<std::vector<char>>(size_bytes);
    } else {
        // 非消费模式：必须从 arena 分配，且拿不到就阻塞等待
        if (!arena_data_ || arena_size_ == 0) {
            throw std::logic_error("FileManagerModule: consume_only=false requires a non-empty arena (memory_bytes>0)");
        }
        // allow_wait=true：阻塞直到可分配，或者 should_stop_
        auto b = AcquireBlock_(size_bytes, /*allow_wait=*/true);
        if (!b.has_value()) {
            throw std::runtime_error("FileManagerModule: stopped while waiting for arena memory");
        }
        block = *b;
        uses_arena = true;
    }

#if defined(__unix__) || defined(__APPLE__)
    const int fd = ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        const int err = errno;
        if (uses_arena) ReleaseBlock_(block);
        throw std::system_error(err, std::generic_category(), "open failed: " + file_path.string());
    }

    char* dst = uses_arena ? (arena_data_ + block.offset) : storage->data();
    std::size_t remaining = size_bytes;
    while (remaining > 0) {
        const ssize_t n = ::read(fd, dst, remaining);
        if (n == 0) {
            ::close(fd);
            if (uses_arena) ReleaseBlock_(block);
            throw std::runtime_error("unexpected EOF while reading: " + file_path.string());
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            const int err = errno;
            ::close(fd);
            if (uses_arena) ReleaseBlock_(block);
            throw std::system_error(err, std::generic_category(), "read failed: " + file_path.string());
        }
        dst += static_cast<std::size_t>(n);
        remaining -= static_cast<std::size_t>(n);
    }

    if (::close(fd) != 0) {
        const int err = errno;
        if (uses_arena) ReleaseBlock_(block);
        throw std::system_error(err, std::generic_category(), "close failed: " + file_path.string());
    }
#else
    // 非 POSIX：使用 iostream 读取
    std::ifstream in(file_path, std::ios::binary);
    if (!in.is_open()) {
        if (uses_arena) ReleaseBlock_(block);
        throw std::runtime_error("open failed: " + file_path.string());
    }
    char* dst = uses_arena ? (arena_data_ + block.offset) : storage->data();
    in.read(dst, static_cast<std::streamsize>(size_bytes));
    if (!in || static_cast<std::size_t>(in.gcount()) != size_bytes) {
        if (uses_arena) ReleaseBlock_(block);
        throw std::runtime_error("read failed: " + file_path.string());
    }
#endif

    FileChunk chunk;
    chunk.size = size_bytes;
    chunk.path = file_path;
    if (uses_arena) {
        chunk.data = arena_data_ + block.offset;
    } else {
        chunk.data = storage->data();
        chunk.storage = std::move(storage);
    }
    return chunk;
}

std::optional<FileManagerModule::Block> FileManagerModule::AcquireBlock_(std::size_t size, bool allow_wait) {
    if (size == 0) return Block{0, 0};

    std::unique_lock<std::mutex> lock(buffer_mu_);

    auto can_alloc = [this, size] {
        if (this->should_stop_) return true;
        return HasBlockFor_(size);
    };

    if (allow_wait) {
        buffer_cv_.wait(lock, can_alloc);
        if (this->should_stop_) return std::nullopt;
        if (!HasBlockFor_(size)) return std::nullopt;
    } else {
        if (!HasBlockFor_(size)) return std::nullopt;
    }

    auto size_it = size_index_.lower_bound(size);
    if (size_it == size_index_.end()) {
        return std::nullopt;
    }

    auto block_it = size_it->second;
    Block chosen = block_it->second;
    size_index_.erase(size_it);
    free_blocks_.erase(block_it);

    if (chosen.size > size) {
        Block remainder{chosen.offset + size, chosen.size - size};
        InsertBlock_(remainder);
        chosen.size = size;
    }
    return chosen;
}

void FileManagerModule::ReleaseBlock_(Block block) {
    if (block.size == 0) return;
    if (block.offset + block.size > arena_size_) {
        throw std::out_of_range("FileManagerModule: block released beyond arena bounds");
    }

    std::unique_lock<std::mutex> lock(buffer_mu_);
    Block merged = block;

    auto next = free_blocks_.lower_bound(merged.offset);
    if (next != free_blocks_.begin()) {
        auto prev = std::prev(next);
        if (prev->second.offset + prev->second.size == merged.offset) {
            merged.offset = prev->second.offset;
            merged.size += prev->second.size;
            RemoveBlock_(prev);
        }
    }

    next = free_blocks_.lower_bound(merged.offset);
    if (next != free_blocks_.end() && merged.offset + merged.size == next->second.offset) {
        merged.size += next->second.size;
        RemoveBlock_(next);
    }

    InsertBlock_(merged);
    lock.unlock();
    buffer_cv_.notify_one();
}

bool FileManagerModule::HasBlockFor_(std::size_t size) const {
    return size_index_.lower_bound(size) != size_index_.end();
}

FileManagerModule::FreeBlocksByOffset::iterator FileManagerModule::InsertBlock_(Block block) {
    auto [it, inserted] = free_blocks_.emplace(block.offset, block);
    if (!inserted) {
        EraseSizeIndexFor_(it);
        it->second.size = block.size;
    }
    size_index_.emplace(it->second.size, it);
    return it;
}

void FileManagerModule::RemoveBlock_(FreeBlocksByOffset::iterator it) {
    EraseSizeIndexFor_(it);
    free_blocks_.erase(it);
}

void FileManagerModule::EraseSizeIndexFor_(FreeBlocksByOffset::iterator it) {
    const std::size_t target_size = it->second.size;
    for (auto range = size_index_.equal_range(target_size); range.first != range.second;) {
        if (range.first->second == it) {
            range.first = size_index_.erase(range.first);
            return;
        }
        ++range.first;
    }
}


