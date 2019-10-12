#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace DiskFileManager {
struct File {
    std::string Filename;
    uint64_t Filesize;
    uint64_t Timestamp;
};

class Archive {
public:
    Archive(const std::filesystem::path& path);
    ~Archive();
    bool IsValid() const;
    const File* GetCurrent();
    void Advance();

private:
    std::vector<File> Files;
    size_t CurrentIndex = 0;
    bool Valid = false;
};
} // namespace DiskFileManager
