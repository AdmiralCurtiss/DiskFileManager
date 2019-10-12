#include <Windows.h>

extern "C" {
#include "totalcmd/functions.h"
#include "totalcmd/wcxhead.h"
}

#include <codecvt>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <locale>
#include <memory>
#include <string>

#include "archive.hpp"

int ToTotalCmdFileTime(int year, int month, int day, int hour, int minute, int second) {
    return (year - 1980) << 25 | month << 21 | day << 16 | hour << 11 | minute << 5 | second / 2;
}

int ToTotalCmdFileTime(uint64_t unix_timestamp) {
    // ??? I can't figure out how to do this properly, this is formally wrong since the epoch is unspecified.
    time_t t;
    t = static_cast<time_t>(unix_timestamp);
    std::tm* dt = std::localtime(&t);
    if (dt) {
        return ToTotalCmdFileTime(dt->tm_year + 1900, dt->tm_mon + 1, dt->tm_mday, dt->tm_hour, dt->tm_min, dt->tm_sec);
    } else {
        return 0;
    }
}

HANDLE __stdcall OpenArchiveW(tOpenArchiveDataW* ArchiveData) {
    try {
        auto arc = std::make_unique<DiskFileManager::Archive>(std::wstring(ArchiveData->ArcName));
        if (arc->IsValid()) {
            return static_cast<HANDLE>(arc.release());
        }
        return nullptr;
    } catch (...) {
        return nullptr;
    }
}

int __stdcall ReadHeaderExW(HANDLE hArcData, tHeaderDataExW* HeaderDataEx) {
    try {
        DiskFileManager::Archive* archive = static_cast<DiskFileManager::Archive*>(hArcData);
        const DiskFileManager::File* file = archive->GetCurrent();
        if (file) {
            std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter; // FIXME: apparently deprecated?
            std::wstring wstr = converter.from_bytes(file->Filename);
            std::replace(wstr.begin(), wstr.end(), L'/', L'\\');

            HeaderDataEx->PackSize = 1;
            HeaderDataEx->PackSizeHigh = 0;
            HeaderDataEx->UnpSize = static_cast<unsigned int>(file->Filesize & 0xFFFFFFFF);
            HeaderDataEx->UnpSizeHigh = static_cast<unsigned int>((file->Filesize >> 32) & 0xFFFFFFFF);
            HeaderDataEx->FileTime = ToTotalCmdFileTime(file->Timestamp);
            std::wstring arcname(L"");
            std::wmemcpy(HeaderDataEx->ArcName, arcname.c_str(), arcname.size());
            std::wmemcpy(HeaderDataEx->FileName, wstr.c_str(), wstr.size());

            return 0;
        } else {
            return E_END_ARCHIVE;
        }
    } catch (...) {
        return E_BAD_ARCHIVE;
    }
}

int __stdcall ProcessFileW(HANDLE hArcData, int Operation, WCHAR* DestPath, WCHAR* DestName) {
    try {
        DiskFileManager::Archive* archive = static_cast<DiskFileManager::Archive*>(hArcData);
        if (Operation == PK_SKIP || Operation == PK_TEST) {
            archive->Advance();
            return 0;
        } else if (Operation == PK_EXTRACT) {
            const DiskFileManager::File* file = archive->GetCurrent();

            int errcode = 0;
            {
                std::ofstream of;
                if (DestPath == NULL) {
                    of = std::ofstream(DestName, std::ios_base::out | std::ios_base::binary);
                } else {
                    of = std::ofstream(std::wstring(DestPath) + L'/' + std::wstring(DestName),
                                       std::ios_base::out | std::ios_base::binary);
                }

                if (of.good()) {
                    of << file->Filesize << std::endl << file->Filename << std::flush;
                    if (of.good()) {
                        of.close();
                    } else {
                        errcode = E_EWRITE;
                    }
                } else {
                    errcode = E_ECREATE;
                }
            }

            archive->Advance();
            return errcode;
        } else {
            return E_NOT_SUPPORTED;
        }
    } catch (...) {
        return E_BAD_ARCHIVE;
    }
}

int __stdcall CloseArchive(HANDLE hArcData) {
    try {
        delete static_cast<DiskFileManager::Archive*>(hArcData);
        return 0;
    } catch (...) {
        return 0;
    }
}

void __stdcall SetChangeVolProcW(HANDLE hArcData, tChangeVolProcW pChangeVolProc1) {}

void __stdcall SetProcessDataProcW(HANDLE hArcData, tProcessDataProcW pProcessDataProc) {}
