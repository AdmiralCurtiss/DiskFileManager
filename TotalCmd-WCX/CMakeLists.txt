cmake_minimum_required(VERSION 3.12)

project(DiskFileManager LANGUAGES C CXX)

add_library(diskfilemanager SHARED)
target_sources(
	diskfilemanager
	PRIVATE
		archive.cpp
		archive.hpp
		plugin.cpp
		wrapper.cpp

		sqlite/shell.c
		sqlite/sqlite3.c
		sqlite/sqlite3.h
		sqlite/sqlite3ext.h

		totalcmd/functions.h
		totalcmd/wcxhead.h
)

set_property(TARGET diskfilemanager PROPERTY PREFIX "")
set_property(TARGET diskfilemanager PROPERTY SUFFIX ".wcx64")
target_compile_features(diskfilemanager PUBLIC cxx_std_17)
