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
        // Allocating 2x input size drastically reduces collision chains (Linear Probing)
        // This trades RAM for raw CPU speed.
        size_t target = input_size * 2;
        
        // Round up to nearest power of 2 for fast bitwise masking (idx & mask)
        capacity = 1;
        while (capacity < target) capacity <<= 1;
        if (capacity < 1024) capacity = 1024; 
        
        mask = capacity - 1;
        size = 0;

        // Aligned Allocation for AVX compatibility (future proofing)
        keys = (int*)alloc_aligned(capacity * sizeof(int));
        values = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity * sizeof(uint8_t));

        // Initialize memory
        if (keys && values && occupied) {
            std::memset(occupied, 0, capacity * sizeof(uint8_t));
            std::memset(values, 0, capacity * sizeof(int64_t));
        }
    }

    ~NativeHashMap() {
        free_aligned(keys);
        free_aligned(values);
        free_aligned(occupied);
    }

    // THE HOT LOOP
    // Fuses "Hash" + "Lookup" + "Insert/Update" into a single tight loop.
    void aggregate_batch(int* input_keys, int* input_vals, size_t count) {
        for (size_t i = 0; i < count; i++) {
            int key = input_keys[i];
            int val = input_vals[i];

            // [HASH ALGORITHM] Murmur3 Avalanche Mixer (Finalizer)
            // Faster than CRC32 for simple 32-bit integer keys.
            uint32_t k = (uint32_t)key;
            k ^= k >> 16;
            k *= 0x85ebca6b;
            k ^= k >> 13;
            k *= 0xc2b2ae35;
            k ^= k >> 16;
            
            size_t idx = k & mask;

            // [COLLISION RESOLUTION] Linear Probing
            // If slot is taken, simply check the next slot. 
            // Modern CPUs prefetch this pattern efficiently.
            while (true) {
                // 1. Check if slot is empty
                if (!occupied[idx]) {
                    // Found empty slot -> CLAIM IT
                    occupied[idx] = 1;
                    keys[idx] = key;
                    values[idx] = val; // Initialize sum
                    size++;
                    break;
                }
                
                // 2. Check if key matches (Update existing group)
                if (keys[idx] == key) {
                    values[idx] += val; // Accumulate sum
                    break;
                }

                // 3. Collision -> Probe next slot (Wrap around via mask)
                idx = (idx + 1) & mask;
            }
        }
    }
};

extern "C" {
    // JNI Wrapper: Create Map -> Aggregate -> Return Pointer
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateSumNative(
        JNIEnv* env, jobject obj, jlong keysPtr, jlong valsPtr, jint count
    ) {
        if (keysPtr == 0 || valsPtr == 0 || count <= 0) return 0;

        // Create the map on the C++ Heap
        NativeHashMap* map = new (std::nothrow) NativeHashMap((size_t)count);
        if (!map) return 0; // OOM check

        int* keys = (int*)keysPtr;
        int* vals = (int*)valsPtr;
        
        // Run High-Speed Aggregation
        map->aggregate_batch(keys, vals, (size_t)count);

        return (jlong)map;
    }

    // JNI Wrapper: Cleanup
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeAggregationResultNative(
        JNIEnv* env, jobject obj, jlong mapPtr
    ) {
        if (mapPtr != 0) {
            delete (NativeHashMap*)mapPtr;
        }
    }
}