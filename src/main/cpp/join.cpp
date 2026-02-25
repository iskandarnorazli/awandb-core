/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 * * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include "common.h"
#include <cstring>
#include <cstdio>

#include <new>
#include <exception>

// Helper to throw a Java OutOfMemoryError from C++
inline void throwJavaOOM(JNIEnv* env, const char* message) {
    jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
    if (exceptionClass != nullptr) {
        env->ThrowNew(exceptionClass, message);
    }
}

// ---------------------------------------------------------
// NATIVE JOIN HASH TABLE
// Optimized for: Primary Key - Foreign Key Joins (Unique Build Keys)
// ---------------------------------------------------------
struct NativeJoinMap {
    int* keys;          // Join Key (e.g., UserID)
    int64_t* payloads;  // Payload (e.g., Pointer to Row, or Value)
    uint8_t* occupied;  
    
    size_t capacity;
    size_t mask;

    NativeJoinMap(size_t input_size) {
        // Target Load Factor < 0.5 for speed
        size_t target = input_size * 2;
        capacity = 1024;
        while (capacity < target) capacity <<= 1;
        mask = capacity - 1;

        keys = (int*)alloc_aligned(capacity * sizeof(int));
        payloads = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity);

        if (!keys || !payloads || !occupied) {
            free_aligned(keys);
            free_aligned(payloads);
            free_aligned(occupied);
            throw std::bad_alloc();
        }
        
        // [CRITICAL FIX] Explicitly zero ALL memory.
        // alloc_aligned does NOT guarantee zeroed memory. 
        // Without this, garbage 'occupied' flags cause ghost matches.
        if (keys) std::memset(keys, 0, capacity * sizeof(int));
        if (payloads) std::memset(payloads, 0, capacity * sizeof(int64_t));
        if (occupied) std::memset(occupied, 0, capacity);
        
        printf("[NativeJoinMap-v2] Initialized Clean Map. Capacity: %zu\n", capacity);
    }

    ~NativeJoinMap() {
        free_aligned(keys);
        free_aligned(payloads);
        free_aligned(occupied);
    }

    // BUILD PHASE: Insert Key + Payload
    void insert_batch(int* inKeys, int64_t* inPayloads, size_t count) {
        for (size_t i = 0; i < count; i++) {
            int key = inKeys[i];
            int64_t val = inPayloads[i];

            uint32_t k = (uint32_t)key;
            k ^= k >> 16; k *= 0x85ebca6b; k ^= k >> 13; k *= 0xc2b2ae35; k ^= k >> 16;
            size_t idx = k & mask;

            while (true) {
                if (!occupied[idx]) {
                    occupied[idx] = 1;
                    keys[idx] = key;
                    payloads[idx] = val;
                    break;
                }
                // Overwrite duplicates (Last Write Wins)
                if (keys[idx] == key) {
                    payloads[idx] = val;
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
    }

    // PROBE PHASE: Lookup Keys -> Output Matches
    int32_t probe_batch(
        int* probeKeys, 
        size_t count, 
        int64_t* outPayloads, 
        int* outProbeIndices
    ) {
        int32_t matches = 0;

        for (size_t i = 0; i < count; i++) {
            int key = probeKeys[i];
            
            uint32_t k = (uint32_t)key;
            k ^= k >> 16; k *= 0x85ebca6b; k ^= k >> 13; k *= 0xc2b2ae35; k ^= k >> 16;
            size_t idx = k & mask;

            while (occupied[idx]) {
                if (keys[idx] == key) {
                    // MATCH FOUND
                    outPayloads[matches] = payloads[idx];
                    outProbeIndices[matches] = (int)i; // Keep track of which probe row matched
                    matches++;
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
        return matches;
    }
};

extern "C" {
    // --- JNI WRAPPERS ---

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_joinBuildNative(
        JNIEnv* env, jobject obj, jlong keysPtr, jlong payloadsPtr, jint count
    ) {
        if (count <= 0) return 0;
        
        try {
            // [CRITICAL FIX] Removed (std::nothrow)
            NativeJoinMap* map = new NativeJoinMap((size_t)count);

            map->insert_batch((int*)keysPtr, (int64_t*)payloadsPtr, (size_t)count);
            return (jlong)map;

        } catch (const std::bad_alloc& e) {
            // Tell the JVM to throw an OutOfMemoryError
            throwJavaOOM(env, "Native memory allocation failed in NativeJoinMap (joinBuildNative)");
            
            return 0;
        }
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_joinProbeNative(
        JNIEnv* env, jobject obj, 
        jlong mapPtr, 
        jlong probeKeysPtr, 
        jint count, 
        jlong outPayloadsPtr, 
        jlong outIndicesPtr
    ) {
        if (mapPtr == 0 || probeKeysPtr == 0) return 0;
        NativeJoinMap* map = (NativeJoinMap*)mapPtr;
        
        return map->probe_batch(
            (int*)probeKeysPtr, 
            (size_t)count, 
            (int64_t*)outPayloadsPtr, 
            (int*)outIndicesPtr
        );
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_joinDestroyNative(
        JNIEnv* env, jobject obj, jlong mapPtr
    ) {
        if (mapPtr != 0) delete (NativeJoinMap*)mapPtr;
    }
}