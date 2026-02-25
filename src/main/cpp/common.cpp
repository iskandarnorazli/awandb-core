/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include "common.h"
#include <cstring> // for std::memset, std::memcpy
#include <cstdlib> // [FIX] Required for posix_memalign & free on Linux/Mac
#include <new>
#include <exception>

void* alloc_aligned(size_t size) {
    // 64-byte alignment is optimal for:
    // 1. Cache Lines (64 bytes on most x86/ARM CPUs)
    // 2. AVX2/AVX-512 registers (32/64 bytes)
    // 3. NEON registers (16 bytes, so 64 is safe)
    size_t alignment = 64; 
#ifdef _WIN32
    return _aligned_malloc(size, alignment);
#else
    void* ptr = nullptr;
    // posix_memalign returns 0 on success
    if (posix_memalign(&ptr, alignment, size) != 0) return nullptr;
    return ptr;
#endif
}

void free_aligned(void* ptr) {
    if (!ptr) return;
#ifdef _WIN32
    _aligned_free(ptr);
#else
    free(ptr);
#endif
}

extern "C" {
    // JNI Wrappers for Memory
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_allocMainStoreNative(JNIEnv* env, jobject obj, jlong num_elements) {
        // [SAFETY] Prevent negative allocation
        if (num_elements < 0) return 0;

        size_t bytes = (size_t)num_elements * sizeof(int);
        void* ptr = alloc_aligned(bytes);
        
        // [SAFETY] Always zero-init to prevent "Ghost Data" bugs
        if (ptr) std::memset(ptr, 0, bytes);
        return (jlong)ptr;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeMainStoreNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) free_aligned((void*)ptr);
    }

    // Allows Scala to get a pointer to an offset without copying data.
    // Critical for Zero-Copy Arrow Flight.
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getOffsetPointerNative(
        JNIEnv* env, jobject obj, jlong basePtr, jlong offsetBytes
    ) {
        if (basePtr == 0) return 0;
        return basePtr + offsetBytes; 
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_memcpyNative(
        JNIEnv* env, jobject obj, jlong srcPtr, jlong dstPtr, jlong bytes
    ) {
        if (srcPtr && dstPtr && bytes > 0) {
            std::memcpy((void*)dstPtr, (void*)srcPtr, (size_t)bytes);
        }
    }

    // Returns the stride (bytes per row) of a column: 1, 2, or 4.
    // This allows TableScanOperator to calculate offsets correctly.
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_getColumnStrideNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx
    ) {
        if (blockPtr == 0) return 0;
        
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)rawPtr;
        
        // [SAFETY FIX] Add Bounds Check
        if (colIdx < 0 || colIdx >= (int)header->column_count) return 0;
        
        // Jump to Column Headers
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        
        // Return stride (populated during creation/loading)
        return (jint)colHeaders[colIdx].stride;
    }
}