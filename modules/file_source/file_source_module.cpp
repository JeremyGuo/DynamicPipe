#include "file_source_module.h"

#include <algorithm>
#include <iostream>
#include <system_error>
#include <utility>

namespace fs = std::filesystem;

static fs::path TryWeaklyCanonical(const fs::path& p) {
    std::error_code ec;
    fs::path out = fs::weakly_canonical(p, ec);
    if (ec) {
        // fallback：至少做 lexical_normal，避免简单的 ".." 干扰
        out = p.lexically_normal();
    }
    return out;
}

FileSourceModule::FileSourceModule(const std::string& path,
                                   const std::vector<std::string>& exclude_paths) {
    InitExclude_(exclude_paths);
    BuildFilesFromPath_(path);
}

FileSourceModule::FileSourceModule(const std::vector<std::string>& file_paths,
                                   const std::vector<std::string>& exclude_paths) {
    InitExclude_(exclude_paths);
    BuildFilesFromList_(file_paths);
}

FileSourceModule::~FileSourceModule() {
    stop_.store(true);
    if (producer_.joinable()) {
        producer_.join();
    }
    // SourcePipeline 析构也会 CloseOutput；这里不重复调用，避免无谓唤醒。
}

void FileSourceModule::StartAfterMemory(const AutoScaleOptions& /*opt*/) {
    bool expected = false;
    if (!producer_started_.compare_exchange_strong(expected, true)) {
        return;
    }
    producer_ = std::thread([this]() { Produce_(); });
}

FileSourceModule::Statistics FileSourceModule::GetStats() const {
    Statistics s;
    std::lock_guard<std::mutex> lock(mu_);
    s.total_files = static_cast<std::uint64_t>(sorted_files_.size());
    s.files_emitted = emitted_;
    return s;
}

void FileSourceModule::InitExclude_(const std::vector<std::string>& exclude_paths) {
    exclude_paths_.clear();
    exclude_paths_.reserve(exclude_paths.size());
    for (const auto& p : exclude_paths) {
        exclude_paths_.push_back(TryWeaklyCanonical(fs::path(p)));
    }
}

void FileSourceModule::BuildFilesFromPath_(const std::string& path) {
    fs::path p = TryWeaklyCanonical(fs::path(path));
    std::error_code ec;

    if (!fs::exists(p, ec) || ec) {
        throw std::runtime_error("FileSourceModule: path does not exist: " + path);
    }

    std::vector<fs::path> files;
    if (fs::is_regular_file(p, ec) && !ec) {
        files.push_back(p);
    } else if (fs::is_directory(p, ec) && !ec) {
        CollectFilesFromDirectory_(p, files);
    } else {
        throw std::runtime_error("FileSourceModule: path is neither file nor directory: " + path);
    }

    // filter + sort by size desc
    std::vector<std::pair<std::uintmax_t, fs::path>> pairs;
    pairs.reserve(files.size());
    for (auto& f : files) {
        if (ShouldExclude_(f)) continue;
        pairs.emplace_back(GetFileSize_(f), std::move(f));
    }
    std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) { return a.first > b.first; });

    sorted_files_.clear();
    sorted_files_.reserve(pairs.size());
    for (auto& kv : pairs) {
        sorted_files_.push_back(std::move(kv.second));
    }
}

void FileSourceModule::BuildFilesFromList_(const std::vector<std::string>& file_paths) {
    std::vector<fs::path> files;
    files.reserve(file_paths.size());
    for (const auto& s : file_paths) {
        files.push_back(TryWeaklyCanonical(fs::path(s)));
    }

    std::vector<std::pair<std::uintmax_t, fs::path>> pairs;
    pairs.reserve(files.size());
    for (auto& f : files) {
        std::error_code ec;
        if (!fs::exists(f, ec) || ec) {
            std::cerr << "FileSourceModule: warning: file does not exist: " << f.string() << "\n";
            continue;
        }
        if (!fs::is_regular_file(f, ec) || ec) {
            std::cerr << "FileSourceModule: warning: not a regular file: " << f.string() << "\n";
            continue;
        }
        if (ShouldExclude_(f)) continue;
        pairs.emplace_back(GetFileSize_(f), std::move(f));
    }

    std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) { return a.first > b.first; });

    sorted_files_.clear();
    sorted_files_.reserve(pairs.size());
    for (auto& kv : pairs) {
        sorted_files_.push_back(std::move(kv.second));
    }
}

void FileSourceModule::CollectFilesFromDirectory_(const fs::path& dir_path, std::vector<fs::path>& files) {
    std::error_code ec;
    fs::recursive_directory_iterator it(dir_path, ec), end;
    if (ec) {
        std::cerr << "FileSourceModule: warning: cannot traverse dir: " << dir_path.string()
                  << " error=" << ec.message() << "\n";
        return;
    }
    for (; it != end; it.increment(ec)) {
        if (ec) {
            std::cerr << "FileSourceModule: warning: traverse error under: " << dir_path.string()
                      << " error=" << ec.message() << "\n";
            ec.clear();
            continue;
        }
        if (it->is_regular_file(ec) && !ec) {
            files.push_back(TryWeaklyCanonical(it->path()));
        }
    }
}

static bool IsSubPathElements(const fs::path& file, const fs::path& root) {
    // 逐元素前缀匹配：root = [a,b], file=[a,b,c] => true
    auto fi = file.begin();
    auto ri = root.begin();
    for (; ri != root.end(); ++ri, ++fi) {
        if (fi == file.end()) return false;
        if (*fi != *ri) return false;
    }
    return true;
}

bool FileSourceModule::ShouldExclude_(const fs::path& file_path) const {
    if (exclude_paths_.empty()) return false;
    const fs::path f = TryWeaklyCanonical(file_path);
    for (const auto& ex : exclude_paths_) {
        if (f == ex) return true;
        std::error_code ec;
        if (fs::is_directory(ex, ec) && !ec) {
            if (IsSubPathElements(f, ex)) return true;
        } else {
            // 非目录：做字符串前缀兜底（兼容某些路径解析失败）
            const std::string fs_ = f.string();
            const std::string exs = ex.string();
            if (!exs.empty() && fs_.rfind(exs, 0) == 0) return true;
        }
    }
    return false;
}

std::uintmax_t FileSourceModule::GetFileSize_(const fs::path& file_path) const {
    std::error_code ec;
    const auto s = fs::file_size(file_path, ec);
    if (ec) return 0;
    return s;
}

void FileSourceModule::Produce_() {
    for (std::size_t i = 0; i < sorted_files_.size(); ++i) {
        if (stop_.load()) break;
        Emit(sorted_files_[i].string());
        {
            std::lock_guard<std::mutex> lock(mu_);
            emitted_++;
        }
    }
    CloseOutput();
}



