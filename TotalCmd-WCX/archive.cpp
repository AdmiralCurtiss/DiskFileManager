#include "archive.hpp"

#include <filesystem>
#include <map>
#include <sstream>

#include "sqlite/sqlite3.h"

DiskFileManager::Archive::Archive(const std::filesystem::path& path) {
    // TODO: Proper error handling!!!!

    Valid = false;
    sqlite3* sqlite_handle; // FIXME: exception safe cleanup for this!!!
    int errcode = sqlite3_open_v2(path.u8string().c_str(), &sqlite_handle, SQLITE_OPEN_READONLY, nullptr);
    if (errcode != SQLITE_OK) {
        sqlite3_close(sqlite_handle);
        return;
    }

    const char* select_statement_volumes = "SELECT id, label FROM Volumes";
    const char* select_statement_files =
        "SELECT Files.id, Files.size, Files.shorthash, Files.hash, Storage.id AS storageId, Pathnames.name AS "
        "pathname, Filenames.name AS filename, Storage.timestamp, Storage.lastSeen, Paths.volumeId AS volumeId FROM "
        "Storage INNER JOIN Files ON Storage.fileId = Files.id INNER JOIN Paths ON Storage.pathId = Paths.id INNER "
        "JOIN Pathnames ON Paths.pathnameId = Pathnames.id INNER JOIN Filenames ON Storage.filenameId = Filenames.id";

    std::map<uint64_t, std::string> volume_labels;
    {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(sqlite_handle, select_statement_volumes, -1, &stmt, nullptr);

        while (true) {
            int step = sqlite3_step(stmt);
            if (step == SQLITE_ROW) {
                uint64_t id = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
                const unsigned char* label = sqlite3_column_text(stmt, 1);
                volume_labels.emplace(id, std::string(reinterpret_cast<const char*>(label)));
            } else {
                break;
            }
        }

        sqlite3_finalize(stmt);
    }
    {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(sqlite_handle, select_statement_files, -1, &stmt, nullptr);

        while (true) {
            int step = sqlite3_step(stmt);
            if (step == SQLITE_ROW) {
                uint64_t size = static_cast<uint64_t>(sqlite3_column_int64(stmt, 1));
                const unsigned char* path = sqlite3_column_text(stmt, 5);
                const unsigned char* file = sqlite3_column_text(stmt, 6);
                uint64_t time = static_cast<uint64_t>(sqlite3_column_int64(stmt, 7));
                uint64_t volume = static_cast<uint64_t>(sqlite3_column_int64(stmt, 9));

                auto vol = volume_labels.find(volume);
                if (vol == volume_labels.end()) {
                    // invalid database, no matching volume
                    continue;
                }

                std::stringstream filename;
                filename << vol->second;
                if (path[0] == '\0' || (path[0] == '/' && path[1] == '\0')) {
                } else if (path[0] == '/') {
                    filename << path;
                } else {
                    filename << "/";
                    filename << path;
                }
                filename << "/";
                filename << file;

                File& f = Files.emplace_back();
                f.Filename = filename.str();
                f.Filesize = size;
                f.Timestamp = time;
            } else {
                break;
            }
        }

        sqlite3_finalize(stmt);
    }


    Valid = true;
    CurrentIndex = 0;
}

DiskFileManager::Archive::~Archive() {}

bool DiskFileManager::Archive::IsValid() const {
    return Valid;
}

const DiskFileManager::File* DiskFileManager::Archive::GetCurrent() {
    if (CurrentIndex < Files.size()) {
        return &Files[CurrentIndex];
    }
    return nullptr;
}

void DiskFileManager::Archive::Advance() {
    ++CurrentIndex;
}
