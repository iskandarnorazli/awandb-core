# Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)

project(awan_engine_core LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/O2 /arch:AVX2 /Oi /GL)
else()
    add_compile_options(-O3 -mavx2 -march=native -fPIC)
endif()

# JNI Path Logic (Keep your existing path logic here)
set(JAVA_HOME_CUSTOM "C:/Program Files/Java/jdk-24") 
if(EXISTS "${JAVA_HOME_CUSTOM}/include/jni.h")
    include_directories("${JAVA_HOME_CUSTOM}/include")
    include_directories("${JAVA_HOME_CUSTOM}/include/win32")
else()
    find_package(JNI REQUIRED)
    include_directories(${JNI_INCLUDE_DIRS})
endif()

# SOURCE FILES (The Full Modular List)
set(SOURCES 
    common.cpp
    storage.cpp      # Block, Data Loading, IO, Metadata
    compute.cpp      # Scan, Strings, Vectors, Arrays
    sort.cpp         # Radix Sort
    hardware.cpp     # Topology
    cuckoo_jni.cpp   # Cuckoo Filter (Updated)
    aggregation.cpp  # Hash Aggregation
    dictionary.cpp  # Dictionary Encoding
    avx_scan.cpp     # AVX2 Scan Operations
)

add_library(awan_engine_core SHARED ${SOURCES})

target_include_directories(awan_engine_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(awan_engine_core PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
)