#pragma once

#include "wcxhead.h"

__declspec(dllexport) HANDLE __stdcall OpenArchive(tOpenArchiveData* ArchiveData);
__declspec(dllexport) HANDLE __stdcall OpenArchiveW(tOpenArchiveDataW* ArchiveData);

__declspec(dllexport) int __stdcall ReadHeader(HANDLE hArcData, tHeaderData* HeaderDataEx);
__declspec(dllexport) int __stdcall ReadHeaderEx(HANDLE hArcData, tHeaderDataEx* HeaderDataEx);
__declspec(dllexport) int __stdcall ReadHeaderExW(HANDLE hArcData, tHeaderDataExW* HeaderDataEx);

__declspec(dllexport) int __stdcall ProcessFile(HANDLE hArcData, int Operation, char* DestPath, char* DestName);
__declspec(dllexport) int __stdcall ProcessFileW(HANDLE hArcData, int Operation, WCHAR* DestPath, WCHAR* DestName);

__declspec(dllexport) int __stdcall CloseArchive(HANDLE hArcData);

__declspec(dllexport) void __stdcall SetChangeVolProc(HANDLE hArcData, tChangeVolProc pChangeVolProc1);
__declspec(dllexport) void __stdcall SetChangeVolProcW(HANDLE hArcData, tChangeVolProcW pChangeVolProc1);

__declspec(dllexport) void __stdcall SetProcessDataProc(HANDLE hArcData, tProcessDataProc pProcessDataProc);
__declspec(dllexport) void __stdcall SetProcessDataProcW(HANDLE hArcData, tProcessDataProcW pProcessDataProc);
