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

void* alloc_aligned(size_t size) {
    size_t alignment = 64; 
#ifdef _WIN32
    return _aligned_malloc(size, alignment);
#else
    void* ptr = nullptr;
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
        size_t bytes = (size_t)num_elements * sizeof(int);
        void* ptr = alloc_aligned(bytes);
        if (ptr) std::memset(ptr, 0, bytes);
        return (jlong)ptr;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeMainStoreNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) free_aligned((void*)ptr);
    }

    // Allows Scala to get a pointer to an offset without copying data.
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getOffsetPointerNative(
        JNIEnv* env, jobject obj, jlong basePtr, jlong offsetBytes
    ) {
        // Just return the address + offset
        return basePtr + offsetBytes; 
    }
}