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

// Helper to throw a Java OutOfMemoryError from C++
inline void throwJavaOOM(JNIEnv* env, const char* message) {
    jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
    if (exceptionClass != nullptr) env->ThrowNew(exceptionClass, message);
}

// ---------------------------------------------------------
// HIGH-PERFORMANCE HASH MAP
// ---------------------------------------------------------
struct NativeHashMap {
    int* keys;          
    int64_t* values;    
    uint8_t* occupied;  
    
    size_t capacity;
    size_t mask;
    size_t size;

    NativeHashMap(size_t initial_capacity = 8192) {
        capacity = initial_capacity;
        mask = capacity - 1;
        size = 0;

        keys = (int*)alloc_aligned(capacity * sizeof(int));
        values = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity * sizeof(uint8_t));

        // [CRITICAL FIX] Abort gracefully if memory is exhausted
        if (!keys || !values || !occupied) {
            free_aligned(keys);
            free_aligned(values);
            free_aligned(occupied);
            throw std::bad_alloc();
        }

        std::memset(occupied, 0, capacity * sizeof(uint8_t));
    }

    ~NativeHashMap() {
        free_aligned(keys);
        free_aligned(values);
        free_aligned(occupied);
    }

    void resize() {
        size_t old_capacity = capacity;
        int* old_keys = keys;
        int64_t* old_values = values;
        uint8_t* old_occupied = occupied;

        capacity <<= 1;
        mask = capacity - 1;

        keys = (int*)alloc_aligned(capacity * sizeof(int));
        values = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity * sizeof(uint8_t));

        // [CRITICAL FIX] Check allocations during map resizing!
        if (!keys || !values || !occupied) {
            free_aligned(keys); free_aligned(values); free_aligned(occupied);
            // Restore old pointers so destructor doesn't double-free or leak
            keys = old_keys; values = old_values; occupied = old_occupied;
            throw std::bad_alloc();
        }

        std::memset(occupied, 0, capacity * sizeof(uint8_t));

        for (size_t i = 0; i < old_capacity; i++) {
            if (old_occupied[i]) {
                int key = old_keys[i];
                int64_t val = old_values[i];
                
                uint32_t k = (uint32_t)key;
                k ^= k >> 16; k *= 0x85ebca6b; k ^= k >> 13; k *= 0xc2b2ae35; k ^= k >> 16;
                size_t idx = k & mask;

                while (true) {
                    if (!occupied[idx]) {
                        occupied[idx] = 1;
                        keys[idx] = key;
                        values[idx] = val;
                        break; 
                    }
                    idx = (idx + 1) & mask;
                }
            }
        }
        free_aligned(old_keys);
        free_aligned(old_values);
        free_aligned(old_occupied);
    }

    void aggregate_batch(int* input_keys, int* input_vals, size_t count) {
        for (size_t i = 0; i < count; i++) {
            int key = input_keys[i];
            int val = input_vals[i];

            uint32_t k = (uint32_t)key;
            k ^= k >> 16; k *= 0x85ebca6b; k ^= k >> 13; k *= 0xc2b2ae35; k ^= k >> 16;
            size_t idx = k & mask;

            while (true) {
                if (!occupied[idx]) {
                    if (size >= capacity / 2) {
                        resize(); // Might throw std::bad_alloc
                        idx = k & mask;
                        continue; 
                    }
                    occupied[idx] = 1;
                    keys[idx] = key;
                    values[idx] = val;
                    size++;
                    break;
                }
                
                if (keys[idx] == key) {
                    values[idx] += val;
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
    }

    int32_t export_to_arrays(int* outKeys, int64_t* outValues) {
        int32_t count = 0;
        for (size_t i = 0; i < capacity; i++) {
            if (occupied[i]) {
                outKeys[count] = keys[i];
                outValues[count] = values[i];
                count++;
            }
        }
        return count;
    }
};

extern "C" {
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateSumNative(
        JNIEnv* env, jobject obj, jlong keysPtr, jlong valsPtr, jint count
    ) {
        if (keysPtr == 0 || valsPtr == 0 || count <= 0) return 0;

        try {
            // Safe allocation wrapper
            NativeHashMap* map = new NativeHashMap(8192);
            map->aggregate_batch((int*)keysPtr, (int*)valsPtr, (size_t)count);
            return (jlong)map;
        } catch (const std::bad_alloc& e) {
            throwJavaOOM(env, "Native memory allocation failed in NativeHashMap");
            return 0;
        }
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateExportNative(
        JNIEnv* env, jobject obj, jlong mapPtr, jlong outKeysPtr, jlong outValsPtr
    ) {
        if (mapPtr == 0 || outKeysPtr == 0 || outValsPtr == 0) return 0;
        return ((NativeHashMap*)mapPtr)->export_to_arrays((int*)outKeysPtr, (int64_t*)outValsPtr);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeAggregationResultNative(
        JNIEnv* env, jobject obj, jlong mapPtr
    ) {
        if (mapPtr != 0) delete (NativeHashMap*)mapPtr;
    }
}