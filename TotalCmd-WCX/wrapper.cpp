#include <Windows.h>

extern "C" {
#include "totalcmd/functions.h"
#include "totalcmd/wcxhead.h"
}

// conversion functions from 'fsplugin' example plugin
#define wdirtypemax 1024
#define longnameprefixmax 6

#ifndef countof
#define countof(str) (sizeof(str) / sizeof(str[0]))
#endif // countof

char* walcopy(char* outname, WCHAR* inname, int maxlen) {
    if (inname) {
        WideCharToMultiByte(CP_ACP, 0, inname, -1, outname, maxlen, NULL, NULL);
        outname[maxlen] = 0;
        return outname;
    } else
        return NULL;
}

WCHAR* awlcopy(WCHAR* outname, char* inname, int maxlen) {
    if (inname) {
        MultiByteToWideChar(CP_ACP, 0, inname, -1, outname, maxlen);
        outname[maxlen] = 0;
        return outname;
    } else
        return NULL;
}

WCHAR* wcslcpy(WCHAR* str1, const WCHAR* str2, int imaxlen) {
    if ((int)wcslen(str2) >= imaxlen - 1) {
        wcsncpy(str1, str2, imaxlen - 1);
        str1[imaxlen - 1] = 0;
    } else
        wcscpy(str1, str2);
    return str1;
}

WCHAR* wcslcat(wchar_t* str1, const WCHAR* str2, int imaxlen) {
    int l1 = (int)wcslen(str1);
    if ((int)wcslen(str2) + l1 >= imaxlen - 1) {
        wcsncpy(str1 + l1, str2, imaxlen - 1 - l1);
        str1[imaxlen - 1] = 0;
    } else
        wcscat(str1, str2);
    return str1;
}

#define wafilenamecopy(outname, inname) walcopy(outname, inname, countof(outname) - 1)
#define awfilenamecopy(outname, inname) awlcopy(outname, inname, countof(outname) - 1)

HANDLE __stdcall OpenArchive(tOpenArchiveData* ArchiveData) {
    WCHAR an[wdirtypemax];
    WCHAR cb[wdirtypemax];

    tOpenArchiveDataW w;
    w.ArcName = awfilenamecopy(an, ArchiveData->ArcName);
    w.OpenMode = ArchiveData->OpenMode;
    w.OpenResult = ArchiveData->OpenResult;
    w.CmtBuf = awfilenamecopy(cb, ArchiveData->CmtBuf);
    w.CmtBufSize = ArchiveData->CmtBufSize;
    w.CmtSize = ArchiveData->CmtSize;
    w.CmtState = ArchiveData->CmtState;
    return OpenArchiveW(&w);
}

int __stdcall ReadHeader(HANDLE hArcData, tHeaderData* HeaderData) {
    tHeaderDataEx h = {};
    int retval = ReadHeaderEx(hArcData, &h);
    memcpy(HeaderData->ArcName, h.ArcName, 260);
    HeaderData->ArcName[260 - 1] = 0;
    memcpy(HeaderData->FileName, h.FileName, 260);
    HeaderData->FileName[260 - 1] = 0;
    HeaderData->Flags = h.Flags;
    HeaderData->PackSize = h.PackSize > INT_MAX ? INT_MAX : static_cast<int>(h.PackSize);
    HeaderData->UnpSize = h.UnpSize > INT_MAX ? INT_MAX : static_cast<int>(h.UnpSize);
    HeaderData->HostOS = h.HostOS;
    HeaderData->FileCRC = h.FileCRC;
    HeaderData->FileTime = h.FileTime;
    HeaderData->UnpVer = h.UnpVer;
    HeaderData->Method = h.Method;
    HeaderData->FileAttr = h.FileAttr;
    HeaderData->CmtBuf = h.CmtBuf;
    HeaderData->CmtBufSize = h.CmtBufSize;
    HeaderData->CmtSize = h.CmtSize;
    HeaderData->CmtState = h.CmtState;
    return retval;
}

int __stdcall ReadHeaderEx(HANDLE hArcData, tHeaderDataEx* HeaderDataEx) {
    tHeaderDataExW h = {};
    int retval = ReadHeaderExW(hArcData, &h);
    wafilenamecopy(HeaderDataEx->ArcName, h.ArcName);
    wafilenamecopy(HeaderDataEx->FileName, h.FileName);
    HeaderDataEx->Flags = h.Flags;
    HeaderDataEx->PackSize = h.PackSize;
    HeaderDataEx->PackSizeHigh = h.PackSizeHigh;
    HeaderDataEx->UnpSize = h.UnpSize;
    HeaderDataEx->UnpSizeHigh = h.UnpSizeHigh;
    HeaderDataEx->HostOS = h.HostOS;
    HeaderDataEx->FileCRC = h.FileCRC;
    HeaderDataEx->FileTime = h.FileTime;
    HeaderDataEx->UnpVer = h.UnpVer;
    HeaderDataEx->Method = h.Method;
    HeaderDataEx->FileAttr = h.FileAttr;
    HeaderDataEx->CmtBuf = h.CmtBuf;
    HeaderDataEx->CmtBufSize = h.CmtBufSize;
    HeaderDataEx->CmtSize = h.CmtSize;
    HeaderDataEx->CmtState = h.CmtState;
    memcpy(HeaderDataEx->Reserved, h.Reserved, 1024);
    return retval;
}

int __stdcall ProcessFile(HANDLE hArcData, int Operation, char* DestPath, char* DestName) {
    WCHAR dp[wdirtypemax];
    WCHAR dn[wdirtypemax];
    return ProcessFileW(hArcData, Operation, awfilenamecopy(dp, DestPath), awfilenamecopy(dn, DestName));
}

void __stdcall SetChangeVolProc(HANDLE hArcData, tChangeVolProc pChangeVolProc1) {
    // FIXME: Forward this.
}

void __stdcall SetProcessDataProc(HANDLE hArcData, tProcessDataProc pProcessDataProc) {
    // FIXME: Forward this.
}
