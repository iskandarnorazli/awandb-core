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

# 1. Project Name (MUST match NativeBridge.scala: "awan_engine_core")
project(awan_engine_core LANGUAGES CXX)

# 2. Standard & Optimization
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/O2 /arch:AVX2 /Oi /GL)
else()
    add_compile_options(-O3 -mavx2 -march=native -fPIC)
endif()

# 3. CRITICAL FIX: JNI Headers (Using your JDK-24 path)
set(JAVA_HOME_CUSTOM "C:/Program Files/Java/jdk-24") 

if(EXISTS "${JAVA_HOME_CUSTOM}/include/jni.h")
    message(STATUS "Forcing JNI Path: ${JAVA_HOME_CUSTOM}")
    include_directories("${JAVA_HOME_CUSTOM}/include")
    include_directories("${JAVA_HOME_CUSTOM}/include/win32")
    link_directories("${JAVA_HOME_CUSTOM}/lib")
else()
    message(WARNING "Custom JDK 24 not found. Falling back to auto-discovery.")
    find_package(JNI REQUIRED)
    include_directories(${JNI_INCLUDE_DIRS})
endif()

# 4. Source Files 
# (engine.cpp is now in the same folder as CMakeLists.txt)
set(SOURCES engine.cpp)

# 5. Create Shared Library
add_library(awan_engine_core SHARED ${SOURCES})

# 6. Include Directories (for block.h)
target_include_directories(awan_engine_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# 7. Output Directory 
# We output to the build folder so VS Code can find it.
# The Task "3. Copy DLL to Core" will handle moving it to the lib folder.
set_target_properties(awan_engine_core PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
)