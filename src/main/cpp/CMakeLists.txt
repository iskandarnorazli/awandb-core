cmake_minimum_required(VERSION 3.20)

project(awan_engine_core LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# =========================================================
# 1. ARCHITECTURE FLAGS (Unchanged)
# =========================================================
if(MSVC)
    if("${CMAKE_SYSTEM_PROCESSOR}" MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
        message(STATUS "Build Mode: Windows x64 (AVX2 Enabled)")
        add_compile_options(/O2 /arch:AVX2 /Oi /GL)
    else()
        add_compile_options(/O2 /Oi /GL)
    endif()
else()
    # Linux/Mac
    add_compile_options(-O3 -march=native -fPIC)
endif()

# =========================================================
# 2. ROBUST JNI CONFIGURATION
# =========================================================

# Step 1: Check for your STABLE Local JDK 24 first (Prevents using messy local caches)
if(WIN32 AND EXISTS "C:/Program Files/Java/jdk-24/include/jni.h")
    set(JAVA_HOME_FINAL "C:/Program Files/Java/jdk-24")
    message(STATUS ">> Local stable JDK 24 detected. Using: ${JAVA_HOME_FINAL}")

# Step 2: Fallback to Environment Variable (Essential for GitHub Actions)
elseif(DEFINED ENV{JAVA_HOME})
    file(TO_CMAKE_PATH "$ENV{JAVA_HOME}" JAVA_HOME_FINAL)
    message(STATUS ">> Using Environment JAVA_HOME: ${JAVA_HOME_FINAL}")

# Step 3: Other fallbacks
else()
    set(JAVA_HOME_FINAL "/usr/lib/jvm/default-java") # Generic Linux
endif()

# Platform-specific subfolder
if(WIN32)
    set(JNI_OS_PLATFORM "win32")
elseif(APPLE)
    set(JNI_OS_PLATFORM "darwin")
else()
    set(JNI_OS_PLATFORM "linux")
endif()

set(JNI_INCLUDES 
    "${JAVA_HOME_FINAL}/include"
    "${JAVA_HOME_FINAL}/include/${JNI_OS_PLATFORM}"
)

# Final validation
if(NOT EXISTS "${JAVA_HOME_FINAL}/include/jni.h")
    message(FATAL_ERROR "!! CRITICAL ERROR: jni.h NOT found at: ${JAVA_HOME_FINAL}/include/jni.h")
endif()

message(STATUS ">> Final JNI Include Paths: ${JNI_INCLUDES}")

# =========================================================
# 3. BUILD TARGETS
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

# Attach JNI paths
target_include_directories(awan_engine_core PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR} 
    ${JNI_INCLUDES}
)

# Output Setup
set_target_properties(awan_engine_core PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
)

# Prevent "lib" prefix on Windows DLLs
if (WIN32)
    set_target_properties(awan_engine_core PROPERTIES PREFIX "")
endif()