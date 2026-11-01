cmake_minimum_required(VERSION 3.14)

project(tuskflow LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(ENABLE_SANITIZER "enable AddressSanitizer on debug build" ON)
if(ENABLE_SANITIZER)
  string(APPEND CMAKE_CXX_FLAGS_DEBUG
    " -fsanitize=address -fno-sanitize-recover=address -fno-omit-frame-pointer")
endif()

find_package(Threads REQUIRED)

# gflags: the system package (libgflags-dev).
find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h REQUIRED)
find_library(GFLAGS_LIBRARY NAMES gflags REQUIRED)
add_library(gflags::gflags UNKNOWN IMPORTED)
set_target_properties(gflags::gflags PROPERTIES
  IMPORTED_LOCATION "${GFLAGS_LIBRARY}"
  INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE_DIR}")

# mimalloc: fetched at configure time, static, without malloc override
# (util/mimalloc_new.cc routes only C++ new/delete to it).
include(FetchContent)
set(MI_BUILD_STATIC ON  CACHE BOOL "" FORCE)
set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(MI_BUILD_OBJECT OFF CACHE BOOL "" FORCE)
set(MI_BUILD_TESTS  OFF CACHE BOOL "" FORCE)
set(MI_OVERRIDE     OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  mimalloc
  GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
  GIT_TAG        v2.3.2
  GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(mimalloc)

function(set_compile_options target_name)
  target_compile_options(${target_name} PRIVATE -Wall -Wextra -Werror)
  # Includes are written from this directory ("tx/transaction.hh").
  target_include_directories(${target_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

# C++ port of the original Java TuskFlow prototype, class by class
# (README_ja.md maps paper Algorithm 1 and the Java classes to the files).
add_executable(tuskflow.exe
  main.cc
  config.cc
  monitor/counters.cc
  monitor/report.cc
  runtime/database.cc
  runtime/query_manager.cc
  runtime/worker.cc
  storage/bomb_loader.cc
  storage/ldbc_loader.cc
  storage/random_loader.cc
  tx/bomb_tx.cc
  tx/mammoth_tx.cc
  tx/short_tx.cc
  tx/task.cc
  tx/transaction.cc
  util/mimalloc_new.cc
  util/thread_random.cc
)
set_compile_options(tuskflow.exe)
target_link_libraries(tuskflow.exe PRIVATE
  Threads::Threads
  gflags::gflags
  mimalloc-static
)

# Serializability checker for --ser_log output.
add_executable(tuskflow_ser_check.exe tools/ser_check.cc)
set_compile_options(tuskflow_ser_check.exe)
