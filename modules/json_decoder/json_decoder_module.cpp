#include "json_decoder_module.h"

#include <cctype>
#include <algorithm>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <optional>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace {

    constexpr unsigned kParseFlags = rapidjson::kParseStopWhenDoneFlag |
                                     rapidjson::kParseValidateEncodingFlag;

    void RemoveProgramListingCodeUnits(DecoderRapidJsonDocument& doc)
    {
        // 确保 document 是对象类型，否则直接返回
        // 注意：如果 JSON 解析后是数组 []、null 或其他非对象类型，IsObject() 会返回 false
        // 在这种情况下，我们不能调用 FindMember 或 MemberEnd，因为它们会断言 IsObject() 为 true
        if (!doc.IsObject()) {
            return;
        }

        // 只有在确认是对象后才调用 FindMember
        // FindMember 内部会调用 MemberEnd()，而 MemberEnd() 会断言 IsObject()
        // 如果 document 不是对象，这里会触发断言失败
        // 
        // 注意：如果使用自定义 allocator，document 的状态应该是一致的
        // 如果 IsObject() 返回 true，那么 document 应该是对象类型
        auto program_listing = doc.FindMember("programListing");
        if (program_listing == doc.MemberEnd()) {
            return;
        }

        // 检查 programListing 的值是否是对象
        if (!program_listing->value.IsObject()) {
            return;
        }

        // 对于嵌套对象，也需要确保它是对象类型
        // 使用 FindMember 而不是直接访问，因为它会检查 IsObject()
        auto code_units = program_listing->value.FindMember("codeUnits");
        if (code_units != program_listing->value.MemberEnd()) {
            program_listing->value.RemoveMember(code_units);
        }
    }

} // namespace

// ==================== DecoderMemoryPool 实现 ====================

DecoderMemoryPool::DecoderMemoryPool()
{
    fallback_mode_ = true;
}

DecoderMemoryPool::DecoderMemoryPool(std::size_t buffer_limit,
                                     const std::string& mmap_file_path,
                                     std::size_t chunk_size)
    : buffer_limit_(buffer_limit), mmap_file_path_(mmap_file_path)
{
    if (buffer_limit_ == 0) {
        throw std::invalid_argument("decoder memory pool size must be greater than zero");
    }

    if (chunk_size == 0) {
        throw std::invalid_argument("chunk size must be greater than zero");
    }

    chunk_size_ = Align(chunk_size);
    if (chunk_size_ > buffer_limit_) {
        throw std::invalid_argument("chunk size cannot be larger than buffer limit");
    }

    use_mmap_ = !mmap_file_path_.empty();
    if (use_mmap_) {
        arena_fd_ = ::open(mmap_file_path_.c_str(), O_RDWR | O_CREAT, 0666);
        if (arena_fd_ < 0) {
            throw std::runtime_error("Failed to open decoder mmap file: " + mmap_file_path_ +
                                     " (" + std::string(strerror(errno)) + ")");
        }

        struct stat st {};
        if (fstat(arena_fd_, &st) < 0) {
            ::close(arena_fd_);
            throw std::runtime_error("Failed to stat decoder mmap file: " + mmap_file_path_);
        }

        const auto file_size = static_cast<std::size_t>(st.st_size);
        if (file_size < buffer_limit_) {
            if (ftruncate(arena_fd_, static_cast<off_t>(buffer_limit_)) < 0) {
                ::close(arena_fd_);
                throw std::runtime_error("Failed to resize decoder mmap file: " + mmap_file_path_ +
                                         " (" + std::string(strerror(errno)) + ")");
            }
        }

        arena_size_ = buffer_limit_;
        arena_data_ = static_cast<char*>(::mmap(nullptr, arena_size_, PROT_READ | PROT_WRITE,
                                                MAP_SHARED, arena_fd_, 0));
        if (arena_data_ == MAP_FAILED) {
            ::close(arena_fd_);
            throw std::runtime_error("Failed to mmap decoder file: " + mmap_file_path_);
        }
    } else {
        arena_storage_.resize(buffer_limit_);
        arena_data_ = arena_storage_.data();
        arena_size_ = arena_storage_.size();
    }

    usable_arena_size_ = arena_size_ - (arena_size_ % Align(1));
    if (usable_arena_size_ < MinBlockSize()) {
        throw std::runtime_error("Decoder memory pool size too small for allocator");
    }

    head_block_ = InitializeFirstBlock();
    free_list_head_ = head_block_;
}

DecoderMemoryPool::~DecoderMemoryPool()
{
    if (use_mmap_ && arena_data_) {
        ::munmap(arena_data_, arena_size_);
    }
    if (arena_fd_ >= 0) {
        ::close(arena_fd_);
    }
}

void* DecoderMemoryPool::Malloc(std::size_t size)
{
    if (size == 0) {
        return nullptr;
    }

    if (fallback_mode_) {
        return std::malloc(size);
    }

    const std::size_t total_size = Align(size) + HeaderSize();

    std::unique_lock<std::mutex> lock(mutex_);

    BlockHeader* block = FindFreeBlock(total_size);

    if (!block) {
        struct WaitGuard {
            explicit WaitGuard(std::function<void(bool)> observer)
                : observer_(std::move(observer))
            {
                if (observer_) {
                    observer_(true);
                }
            }
            ~WaitGuard()
            {
                if (observer_) {
                    observer_(false);
                }
            }
            std::function<void(bool)> observer_;
        } guard(wait_observer_);

        while (!block) {
            alloc_cv_.wait(lock, [&] {
                block = FindFreeBlock(total_size);
                return block != nullptr;
            });
        }
    }

    SplitBlock(block, total_size);
    RemoveFreeBlock(block);
    block->free = false;

    return reinterpret_cast<char*>(block) + HeaderSize();
}

void DecoderMemoryPool::Free(void* ptr)
{
    if (!ptr) {
        return;
    }

    if (fallback_mode_) {
        std::free(ptr);
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!PointerInArena(ptr)) {
        return;
    }

    auto* block = reinterpret_cast<BlockHeader*>(static_cast<char*>(ptr) - HeaderSize());
    if (block->free) {
        return;
    }

    block->free = true;
    block = Coalesce(block);
    InsertFreeBlock(block);

    alloc_cv_.notify_one();
}

DecoderMemoryPool::BlockHeader* DecoderMemoryPool::InitializeFirstBlock()
{
    auto* block = reinterpret_cast<BlockHeader*>(arena_data_);
    block->size = usable_arena_size_;
    block->free = true;
    block->prev = nullptr;
    block->next = nullptr;
    block->prev_free = nullptr;
    block->next_free = nullptr;
    return block;
}

DecoderMemoryPool::BlockHeader* DecoderMemoryPool::FindFreeBlock(std::size_t total_size)
{
    BlockHeader* current = free_list_head_;
    while (current) {
        if (current->size >= total_size) {
            return current;
        }
        current = current->next_free;
    }
    return nullptr;
}

void DecoderMemoryPool::SplitBlock(BlockHeader* block, std::size_t total_size)
{
    if (!block || block->size <= total_size + MinBlockSize()) {
        return;
    }

    char* block_start = reinterpret_cast<char*>(block);
    auto* new_block = reinterpret_cast<BlockHeader*>(block_start + total_size);
    new_block->size = block->size - total_size;
    new_block->free = true;
    new_block->prev = block;
    new_block->next = block->next;
    if (new_block->next) {
        new_block->next->prev = new_block;
    }
    new_block->prev_free = nullptr;
    new_block->next_free = nullptr;

    block->next = new_block;
    block->size = total_size;

    InsertFreeBlock(new_block);
}

void DecoderMemoryPool::InsertFreeBlock(BlockHeader* block)
{
    if (!block) {
        return;
    }

    block->free = true;
    block->prev_free = nullptr;
    block->next_free = free_list_head_;
    if (free_list_head_) {
        free_list_head_->prev_free = block;
    }
    free_list_head_ = block;
}

void DecoderMemoryPool::RemoveFreeBlock(BlockHeader* block)
{
    if (!block || !block->free) {
        return;
    }

    if (block->prev_free) {
        block->prev_free->next_free = block->next_free;
    } else if (free_list_head_ == block) {
        free_list_head_ = block->next_free;
    }

    if (block->next_free) {
        block->next_free->prev_free = block->prev_free;
    }

    block->prev_free = nullptr;
    block->next_free = nullptr;
}

DecoderMemoryPool::BlockHeader* DecoderMemoryPool::Coalesce(BlockHeader* block)
{
    if (!block) {
        return block;
    }

    BlockHeader* prev = block->prev;
    if (prev && prev->free) {
        RemoveFreeBlock(prev);
        prev->size += block->size;
        prev->next = block->next;
        if (block->next) {
            block->next->prev = prev;
        }
        block = prev;
    }

    BlockHeader* next = block->next;
    if (next && next->free) {
        RemoveFreeBlock(next);
        block->size += next->size;
        block->next = next->next;
        if (next->next) {
            next->next->prev = block;
        }
    }

    return block;
}

bool DecoderMemoryPool::PointerInArena(const void* ptr) const
{
    if (!arena_data_ || usable_arena_size_ == 0) {
        return false;
    }

    const char* char_ptr = static_cast<const char*>(ptr);
    const char* start = arena_data_ + HeaderSize();
    const char* end = arena_data_ + usable_arena_size_;
    return char_ptr >= start && char_ptr < end;
}

// ==================== DecodedJson 实现 ====================

DecodedJson::DecodedJson(DecodedJson &&other) noexcept
    : document(std::move(other.document)),
      source_path(std::move(other.source_path)),
      line_number(other.line_number),
      line_length(other.line_length),
      raw_json(other.raw_json),
      raw_json_length(other.raw_json_length),
      allocator_(std::move(other.allocator_)),
      chunk_data_ptr_(other.chunk_data_ptr_),
      released_(other.released_)
{
    other.line_number = 0;
    other.line_length = 0;
    other.raw_json = nullptr;
    other.raw_json_length = 0;
    other.allocator_.reset();
    other.chunk_data_ptr_ = nullptr;
    other.released_ = true;
}

DecodedJson &DecodedJson::operator=(DecodedJson &&other) noexcept
{
    if (this == &other) {
        return *this;
    }
    document = std::move(other.document);
    source_path = std::move(other.source_path);
    line_number = other.line_number;
    line_length = other.line_length;
    raw_json = other.raw_json;
    raw_json_length = other.raw_json_length;
    allocator_ = std::move(other.allocator_);
    chunk_data_ptr_ = other.chunk_data_ptr_;
    released_ = other.released_;

    other.line_number = 0;
    other.line_length = 0;
    other.raw_json = nullptr;
    other.raw_json_length = 0;
    other.chunk_data_ptr_ = nullptr;
    other.released_ = true;

    return *this;
}

// ==================== JsonDecoderModule 实现 ====================

JsonDecoderModule::JsonDecoderModule(ThreadScheduler& scheduler,
                                     SourcePipeline<FileChunk>* upstream,
                                     Options opt)
    : Pipeline<FileChunk, DecodedJson>(scheduler, upstream),
      opt_(std::move(opt))
{
    // chunk size 固定为 decoder_chunk_size_（默认 64KiB），不对外暴露配置
}

JsonDecoderModule::~JsonDecoderModule()
{
    std::vector<FileChunk> chunks_to_release;
    {
        std::lock_guard<std::mutex> lock(ref_count_mutex_);
        for (auto& [data_ptr, chunk_ref] : chunk_refs_) {
            if (chunk_ref && chunk_ref->ref_count > 0) {
                chunks_to_release.push_back(std::move(chunk_ref->chunk));
            }
        }
        chunk_refs_.clear();
    }

    if (auto* upstream = GetUpstream()) {
        for (auto& chunk : chunks_to_release) {
            upstream->Release(std::move(chunk));
        }
    }
}

void JsonDecoderModule::OnMemorySet(std::size_t memory_bytes)
{
    if (memory_bytes == 0) {
        decoder_memory_pool_.reset();
        return;
    }

    decoder_memory_pool_ = std::make_unique<DecoderMemoryPool>(
        memory_bytes,
        opt_.mmap_file_path,
        decoder_chunk_size_);

    decoder_memory_pool_->SetWaitObserver([this](bool entering) {
        thread_local std::optional<MemoryWaitGuard> guard;
        if (entering) {
            guard.emplace(this->TrackMemoryRequest());
        } else {
            guard.reset();
        }
    });
}

void JsonDecoderModule::Release(DecodedJson&& data)
{
    if (!data.released_ && data.chunk_data_ptr_) {
        DecrementRefCount(data.chunk_data_ptr_);
        data.chunk_data_ptr_ = nullptr;
        data.released_ = true;
    }

    data.document.reset();
    data.raw_json = nullptr;
    data.raw_json_length = 0;
    data.line_number = 0;
    data.line_length = 0;
}

void JsonDecoderModule::Process(FileChunk&& chunk)
{
    // 检查文件扩展名，只处理 .json 和 .jsonl 文件
    std::string extension = chunk.path.extension().string();
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (extension != ".json" && extension != ".jsonl") {
        // 不是 JSON 文件，直接释放并返回
        if (auto* upstream = GetUpstream()) {
            upstream->Release(std::move(chunk));
        }
        return;
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        total_chunks_++;
        total_bytes_ += chunk.size;
    }

    if (chunk.data == nullptr || chunk.size == 0) {
        if (auto* upstream = GetUpstream()) {
            upstream->Release(std::move(chunk));
        }
        return;
    }

    const char* data_ptr = chunk.data;
    {
        std::lock_guard<std::mutex> lock(ref_count_mutex_);
        if (chunk_refs_.find(data_ptr) == chunk_refs_.end()) {
            chunk_refs_[data_ptr] = std::make_unique<ChunkRef>(std::move(chunk));
        }
    }

    ChunkRef* chunk_ref = nullptr;
    {
        std::lock_guard<std::mutex> lock(ref_count_mutex_);
        auto it = chunk_refs_.find(data_ptr);
        if (it != chunk_refs_.end()) {
            chunk_ref = it->second.get();
            chunk_ref->ref_count ++;
        }
    }

    if (!chunk_ref) {
        return;
    }

    const char* cursor = chunk_ref->chunk.data;
    const char* const end = cursor + chunk_ref->chunk.size;
    std::size_t line_number = 0;

    while (cursor < end) {
        const char* line_start = cursor;
        while (cursor < end && cursor[0] != '\n' && cursor[0] != '\r') {
            ++cursor;
        }
        const char* line_end = cursor;
        ++line_number;

        while (line_start < line_end && std::isspace(static_cast<unsigned char>(*line_start))) {
            ++line_start;
        }
        while (line_end > line_start && std::isspace(static_cast<unsigned char>(*(line_end - 1)))) {
            --line_end;
        }

        if (line_end > line_start) {
            ProcessLine(line_start, line_end, line_number, data_ptr, chunk_ref->chunk.path, end);
        }

        if (cursor < end) {
            if (*cursor == '\r') {
                ++cursor;
            }
            if (cursor < end && *cursor == '\n') {
                ++cursor;
            }
        }
    }

    ReleaseChunkIfUnused(data_ptr);
}

void JsonDecoderModule::ProcessLine(
    const char* line_start,
    const char* line_end,
    std::size_t line_number,
    const char* chunk_data_ptr,
    const std::filesystem::path& source_path,
    const char* chunk_end
)
{
    const std::size_t trimmed_length = static_cast<std::size_t>(line_end - line_start);
    if (trimmed_length == 0) {
        return;
    }

    char* mutable_line_start = const_cast<char*>(line_start);
    char* mutable_line_end = const_cast<char*>(line_end);
    const bool has_trailing_char = line_end < chunk_end;
    char saved_char = '\0';

    std::unique_ptr<DecoderRapidJsonAllocator> custom_allocator;
    DecoderRapidJsonAllocator* allocator_ptr = nullptr;
    if (decoder_memory_pool_) {
        custom_allocator = std::make_unique<DecoderRapidJsonAllocator>(decoder_chunk_size_, decoder_memory_pool_.get());
        allocator_ptr = custom_allocator.get();
    }

    std::unique_ptr<DecoderRapidJsonDocument> document;
    if (allocator_ptr) {
        document = std::make_unique<DecoderRapidJsonDocument>(allocator_ptr);
    } else {
        document = std::make_unique<DecoderRapidJsonDocument>();
    }
    rapidjson::ParseResult parse_result;

    if (has_trailing_char) {
        saved_char = *mutable_line_end;
        *mutable_line_end = '\0';
        parse_result = document->ParseInsitu<kParseFlags>(mutable_line_start);
        *mutable_line_end = saved_char;
    } else {
        parse_result = document->Parse<kParseFlags>(mutable_line_start, trimmed_length);
    }

    if (!parse_result) {
        return;
    }

    // 只有在 document 是对象时才尝试移除 programListing.codeUnits
    // 如果 JSON 是数组 []、null 或其他非对象类型，IsObject() 会返回 false
    if (document->IsObject()) {
        RemoveProgramListingCodeUnits(*document);
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        decoded_lines_++;
    }

    IncrementRefCount(chunk_data_ptr);

    DecodedJson decoded;
    decoded.allocator_ = std::move(custom_allocator);
    decoded.document = std::move(document);
    decoded.source_path = source_path;
    decoded.line_number = line_number;
    decoded.line_length = trimmed_length;
    decoded.raw_json = line_start;
    decoded.raw_json_length = trimmed_length;
    decoded.chunk_data_ptr_ = chunk_data_ptr;
    decoded.released_ = false;

    Emit(std::move(decoded));
}

void JsonDecoderModule::IncrementRefCount(const char* data_ptr)
{
    if (!data_ptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(ref_count_mutex_);
    auto it = chunk_refs_.find(data_ptr);
    if (it != chunk_refs_.end()) {
        it->second->ref_count++;
    }
}

void JsonDecoderModule::DecrementRefCount(const char* data_ptr)
{
    if (!data_ptr) {
        return;
    }

    FileChunk chunk_to_release;
    bool should_release = false;

    {
        std::lock_guard<std::mutex> lock(ref_count_mutex_);
        auto it = chunk_refs_.find(data_ptr);
        if (it != chunk_refs_.end()) {
            it->second->ref_count--;
            if (it->second->ref_count == 0) {
                chunk_to_release = std::move(it->second->chunk);
                chunk_refs_.erase(it);
                should_release = true;
            }
        }
    }

    if (should_release) {
        if (auto* upstream = GetUpstream()) {
            upstream->Release(std::move(chunk_to_release));
        }
    }
}

void JsonDecoderModule::ReleaseChunkIfUnused(const char* data_ptr)
{
    if (!data_ptr) {
        return;
    }

    FileChunk chunk_to_release;
    bool should_release = false;

    {
        std::lock_guard<std::mutex> lock(ref_count_mutex_);
        auto it = chunk_refs_.find(data_ptr);
        if (it != chunk_refs_.end()) {
            it->second->ref_count--;
            if (it->second->ref_count == 0) {
                chunk_to_release = std::move(it->second->chunk);
                chunk_refs_.erase(it);
                should_release = true;
            }
        }
    }

    if (should_release) {
        if (auto* upstream = GetUpstream()) {
            upstream->Release(std::move(chunk_to_release));
        }
    }
}

JsonDecoderModule::Statistics JsonDecoderModule::GetStats() const
{
    Statistics stats;
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats.chunks = total_chunks_;
        stats.bytes = total_bytes_;
        stats.decoded_lines = decoded_lines_;
    }
    return stats;
}



