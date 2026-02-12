cmake_minimum_required(VERSION 3.20)

project(awan_engine_core LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- 1. ARCHITECTURE FLAGS ---
if(MSVC)
    if("${CMAKE_SYSTEM_PROCESSOR}" MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
        message(STATUS "Build Mode: Windows x64 (AVX2 Enabled)")
        add_compile_options(/O2 /arch:AVX2 /Oi /GL)
    else()
        add_compile_options(/O2 /Oi /GL)
    endif()
else()
    add_compile_options(-O3 -march=native -fPIC)
endif()

# =========================================================
# 2. JNI CONFIGURATION (MANUAL FORCE FIX)
# =========================================================

# TODO: CHECK THIS PATH ON YOUR MACHINE!
set(MY_JAVA_HOME "C:/Program Files/Java/jdk-24") 

if(NOT EXISTS "${MY_JAVA_HOME}/include/jni.h")
    message(WARNING "!!! WARNING: jni.h NOT FOUND at ${MY_JAVA_HOME}/include/jni.h")
    message(WARNING "!!! Please verify your JDK version in 'C:/Program Files/Java/'")
    # Fallback attempt for JDK 21 just in case
    set(MY_JAVA_HOME "C:/Program Files/Java/jdk-21")
endif()

message(STATUS "Force-loading JNI from: ${MY_JAVA_HOME}")

set(JNI_INCLUDES 
    "${MY_JAVA_HOME}/include"
    "${MY_JAVA_HOME}/include/win32"
)

# =========================================================

set(SOURCES 
    common.cpp
    storage.cpp
    compute.cpp
    sort.cpp
    hardware.cpp
    cuckoo_jni.cpp
    aggregation.cpp
    dictionary.cpp
    avx_scan.cpp
    bitpacking.cpp
    join.cpp
)

add_library(awan_engine_core SHARED ${SOURCES})

# [CRITICAL FIX] Attach JNI paths directly to the library target
target_include_directories(awan_engine_core PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${JNI_INCLUDES}
)

set_target_properties(awan_engine_core PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
)

if (WIN32)
    set_target_properties(awan_engine_core PROPERTIES PREFIX "")
endif()