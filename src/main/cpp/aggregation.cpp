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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

#include "common.h"
#include <cstring>
#include <cstdio> // For printf debugging

// ---------------------------------------------------------
// HIGH-PERFORMANCE HASH MAP (Linear Probing)
// Optimized for: GROUP BY key SUM(val)
// ---------------------------------------------------------
struct NativeHashMap {
    int* keys;          // Stored Group IDs (Keys)
    int64_t* values;    // Accumulated Sums (Values)
    uint8_t* occupied;  // Flags: 1 = slot taken, 0 = empty
    
    size_t capacity;
    size_t mask;
    size_t size;

    NativeHashMap(size_t input_size) {
        // [TUNING] Load Factor 0.5
        // Allocating 2x input size drastically reduces collision chains
        size_t target = input_size * 2;
        
        // Round up to nearest power of 2
        capacity = 1024;
        while (capacity < target) capacity <<= 1;
        
        mask = capacity - 1;
        size = 0;

        //printf("[NativeHashMap] Init: Input=%zu, Target=%zu, Capacity=%zu, Mask=%zu\n", input_size, target, capacity, mask);

        // Aligned Allocation for AVX
        keys = (int*)alloc_aligned(capacity * sizeof(int));
        values = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity * sizeof(uint8_t));

        if (keys && values && occupied) {
            std::memset(occupied, 0, capacity * sizeof(uint8_t));
            std::memset(values, 0, capacity * sizeof(int64_t));
        } else {
            printf("[NativeHashMap] ERROR: Allocation Failed!\n");
        }
    }

    ~NativeHashMap() {
        free_aligned(keys);
        free_aligned(values);
        free_aligned(occupied);
    }

    // THE HOT LOOP
    void aggregate_batch(int* input_keys, int* input_vals, size_t count) {
        size_t collisions = 0;
        
        for (size_t i = 0; i < count; i++) {
            int key = input_keys[i];
            int val = input_vals[i];

            // Murmur3 Finalizer Hash
            uint32_t k = (uint32_t)key;
            k ^= k >> 16;
            k *= 0x85ebca6b;
            k ^= k >> 13;
            k *= 0xc2b2ae35;
            k ^= k >> 16;
            
            size_t idx = k & mask;

            while (true) {
                // 1. Empty Slot -> Claim
                if (!occupied[idx]) {
                    occupied[idx] = 1;
                    keys[idx] = key;
                    values[idx] = val;
                    size++;
                    break;
                }
                
                // 2. Match -> Accumulate
                if (keys[idx] == key) {
                    values[idx] += val;
                    break;
                }

                // 3. Collision -> Probe Next
                collisions++;
                idx = (idx + 1) & mask;
            }
        }
        
        if (collisions > count / 10) {
             printf("[NativeHashMap] Batch Processed. Collisions: %zu\n", collisions);
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
        //printf("[NativeHashMap] Exporting %d groups (Internal Size: %zu)\n", count, size);
        return count;
    }
};

extern "C" {
    // --------------------------------------------------------
    // AGGREGATION WRAPPERS ONLY
    // (Do NOT include storage functions here!)
    // --------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateSumNative(
        JNIEnv* env, jobject obj, jlong keysPtr, jlong valsPtr, jint count
    ) {
        if (keysPtr == 0 || valsPtr == 0 || count <= 0) return 0;

        NativeHashMap* map = new (std::nothrow) NativeHashMap((size_t)count);
        if (!map) return 0;

        int* keys = (int*)keysPtr;
        int* vals = (int*)valsPtr;
        
        map->aggregate_batch(keys, vals, (size_t)count);

        return (jlong)map;
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateExportNative(
        JNIEnv* env, jobject obj, jlong mapPtr, jlong outKeysPtr, jlong outValsPtr
    ) {
        if (mapPtr == 0 || outKeysPtr == 0 || outValsPtr == 0) return 0;
        NativeHashMap* map = (NativeHashMap*)mapPtr;
        return map->export_to_arrays((int*)outKeysPtr, (int64_t*)outValsPtr);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeAggregationResultNative(
        JNIEnv* env, jobject obj, jlong mapPtr
    ) {
        if (mapPtr != 0) {
            delete (NativeHashMap*)mapPtr;
        }
    }
}