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
#include <cstring>
#include <cstdio> 
#include <new> // [FIX] Required for std::nothrow
#include "block.h"

// ---------------------------------------------------------
// HIGH-PERFORMANCE HASH MAP (Linear Probing)
// Optimized for: GROUP BY key SUM(val)
// NOTE: This uses Scalar C++ and is cross-platform (Intel/ARM) by default.
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

        // Aligned Allocation for AVX/NEON compatibility
        keys = (int*)alloc_aligned(capacity * sizeof(int));
        values = (int64_t*)alloc_aligned(capacity * sizeof(int64_t));
        occupied = (uint8_t*)alloc_aligned(capacity * sizeof(uint8_t));

        if (keys && values && occupied) {
            // [CRITICAL FIX] Zero-Initialize Memory
            // Without this, garbage 'occupied' flags cause ghost groups to appear.
            std::memset(occupied, 0, capacity * sizeof(uint8_t));
            std::memset(values, 0, capacity * sizeof(int64_t));
            // Keys don't strictly need zeroing if occupied is 0, but good for safety
            std::memset(keys, 0, capacity * sizeof(int)); 
        } else {
            // Handle OOM gracefully if possible, though constructor can't return error easily.
            // Caller checks for null map pointer.
        }
    }

    ~NativeHashMap() {
        free_aligned(keys);
        free_aligned(values);
        free_aligned(occupied);
    }

    // Fused Single-Row Aggregation (Keeps data hot in L1 Cache)
    inline void aggregate_single(int key, int val) {
        // Murmur3 Finalizer Hash
        uint32_t k = (uint32_t)key;
        k ^= k >> 16; k *= 0x85ebca6b; k ^= k >> 13; k *= 0xc2b2ae35; k ^= k >> 16;
        size_t idx = k & mask;

        while (true) {
            if (!occupied[idx]) {
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
        
        // if (collisions > count / 10) printf("[NativeHashMap] High Collisions: %zu\n", collisions);
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

    #ifdef ARCH_ARM
// Helper to extract a 4-bit mask from a 128-bit NEON vector
inline int agg_neon_movemask_epi32(uint32x4_t cmp) {
    return (vgetq_lane_u32(cmp, 0) & 1) |
          ((vgetq_lane_u32(cmp, 1) & 1) << 1) |
          ((vgetq_lane_u32(cmp, 2) & 1) << 2) |
          ((vgetq_lane_u32(cmp, 3) & 1) << 3);
}
#endif
};

extern "C" {
    // --------------------------------------------------------
    // AGGREGATION WRAPPERS ONLY
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
    // ==========================================================
    // OPERATOR FUSION: SIMD Filter -> Gather -> Hash Aggregate
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxFilteredAggregateNative(
        JNIEnv* env, jobject obj, jlong blockPtr, 
        jint filterColIdx, jint opType, jint targetVal,
        jint keyColIdx, jint valColIdx,
        jlong mapPtr, jlong deletedBitmaskPtr
    ) {
        if (blockPtr == 0 || mapPtr == 0) return 0;

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        
        ColumnHeader& filterCol = colHeaders[filterColIdx];
        ColumnHeader& keyCol = colHeaders[keyColIdx];
        ColumnHeader& valCol = colHeaders[valColIdx];

        if (filterCol.type != 0 || keyCol.type != 0 || valCol.type != 0) return 0;
        
        // Zone Map Pruning - skip the entire block if it can't match
        if (opType == 0 && (targetVal < filterCol.min_int || targetVal > filterCol.max_int)) return 0; 
        if (opType == 1 && filterCol.max_int <= targetVal) return 0; 
        if (opType == 2 && filterCol.max_int < targetVal) return 0; 
        if (opType == 3 && filterCol.min_int >= targetVal) return 0; 
        if (opType == 4 && filterCol.min_int > targetVal) return 0; 

        int32_t* filterData = (int32_t*)(basePtr + filterCol.data_offset);
        int32_t* keyData = (int32_t*)(basePtr + keyCol.data_offset);
        int32_t* valData = (int32_t*)(basePtr + valCol.data_offset);
        
        NativeHashMap* map = (NativeHashMap*)mapPtr;
        uint8_t* bitmask = (uint8_t*)deletedBitmaskPtr;
        int rows = header->row_count;
        int matchCount = 0;
        int i = 0;

#ifdef ARCH_X86
        __m256i vTarget = _mm256_set1_epi32(targetVal);
        int limit = rows - 32;
        
        for (; i <= limit; i += 32) {
            __m256i v0 = _mm256_loadu_si256((__m256i*)&filterData[i]);
            __m256i v1 = _mm256_loadu_si256((__m256i*)&filterData[i+8]);
            __m256i v2 = _mm256_loadu_si256((__m256i*)&filterData[i+16]);
            __m256i v3 = _mm256_loadu_si256((__m256i*)&filterData[i+24]);

            __m256i cmp0, cmp1, cmp2, cmp3;
            if (opType == 0) { // ==
                cmp0 = _mm256_cmpeq_epi32(v0, vTarget);
                cmp1 = _mm256_cmpeq_epi32(v1, vTarget);
                cmp2 = _mm256_cmpeq_epi32(v2, vTarget);
                cmp3 = _mm256_cmpeq_epi32(v3, vTarget);
            } else if (opType == 1) { // >
                cmp0 = _mm256_cmpgt_epi32(v0, vTarget);
                cmp1 = _mm256_cmpgt_epi32(v1, vTarget);
                cmp2 = _mm256_cmpgt_epi32(v2, vTarget);
                cmp3 = _mm256_cmpgt_epi32(v3, vTarget);
            } else if (opType == 2) { // >= (implemented as: v > targetVal - 1)
                __m256i vT = _mm256_set1_epi32(targetVal - 1);
                cmp0 = _mm256_cmpgt_epi32(v0, vT);
                cmp1 = _mm256_cmpgt_epi32(v1, vT);
                cmp2 = _mm256_cmpgt_epi32(v2, vT);
                cmp3 = _mm256_cmpgt_epi32(v3, vT);
            } else if (opType == 3) { // < (implemented as: targetVal > v)
                cmp0 = _mm256_cmpgt_epi32(vTarget, v0);
                cmp1 = _mm256_cmpgt_epi32(vTarget, v1);
                cmp2 = _mm256_cmpgt_epi32(vTarget, v2);
                cmp3 = _mm256_cmpgt_epi32(vTarget, v3);
            } else { // <= (implemented as: targetVal + 1 > v)
                __m256i vT = _mm256_set1_epi32(targetVal + 1);
                cmp0 = _mm256_cmpgt_epi32(vT, v0);
                cmp1 = _mm256_cmpgt_epi32(vT, v1);
                cmp2 = _mm256_cmpgt_epi32(vT, v2);
                cmp3 = _mm256_cmpgt_epi32(vT, v3);
            }

            int m0 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp0));
            int m1 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp1));
            int m2 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp2));
            int m3 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp3));

            uint32_t matchMask = (m0) | (m1 << 8) | (m2 << 16) | (m3 << 24);
            uint32_t delMask = (bitmask != nullptr) ? *(uint32_t*)(bitmask + (i >> 3)) : 0;
            uint32_t validMask = matchMask & (~delMask);
            
            while (validMask) {
                unsigned long tz;
#ifdef _MSC_VER
                _BitScanForward(&tz, validMask);
#else
                tz = __builtin_ctz(validMask); 
#endif
                int rowId = i + tz;
                map->aggregate_single(keyData[rowId], valData[rowId]);
                matchCount++;
                validMask &= (validMask - 1); 
            }
        }
#elif defined(ARCH_ARM)
        int32x4_t vTarget = vdupq_n_s32(targetVal);
        int limit = rows - 16;
        for (; i <= limit; i += 16) {
            int32x4_t v0 = vld1q_s32(&filterData[i]);
            int32x4_t v1 = vld1q_s32(&filterData[i+4]);
            int32x4_t v2 = vld1q_s32(&filterData[i+8]);
            int32x4_t v3 = vld1q_s32(&filterData[i+12]);

            uint32x4_t cmp0, cmp1, cmp2, cmp3;
            if (opType == 0) {
                cmp0 = vceqq_s32(v0, vTarget);
                cmp1 = vceqq_s32(v1, vTarget);
                cmp2 = vceqq_s32(v2, vTarget);
                cmp3 = vceqq_s32(v3, vTarget);
            } else if (opType == 1) {
                cmp0 = vcgtq_s32(v0, vTarget);
                cmp1 = vcgtq_s32(v1, vTarget);
                cmp2 = vcgtq_s32(v2, vTarget);
                cmp3 = vcgtq_s32(v3, vTarget);
            } else if (opType == 2) {
                cmp0 = vcgeq_s32(v0, vTarget);
                cmp1 = vcgeq_s32(v1, vTarget);
                cmp2 = vcgeq_s32(v2, vTarget);
                cmp3 = vcgeq_s32(v3, vTarget);
            } else if (opType == 3) {
                cmp0 = vcltq_s32(v0, vTarget);
                cmp1 = vcltq_s32(v1, vTarget);
                cmp2 = vcltq_s32(v2, vTarget);
                cmp3 = vcltq_s32(v3, vTarget);
            } else {
                cmp0 = vcleq_s32(v0, vTarget);
                cmp1 = vcleq_s32(v1, vTarget);
                cmp2 = vcleq_s32(v2, vTarget);
                cmp3 = vcleq_s32(v3, vTarget);
            }

            int m0 = agg_neon_movemask_epi32(cmp0);
            int m1 = agg_neon_movemask_epi32(cmp1);
            int m2 = agg_neon_movemask_epi32(cmp2);
            int m3 = agg_neon_movemask_epi32(cmp3);

            uint32_t matchMask = (m0) | (m1 << 4) | (m2 << 8) | (m3 << 12);
            uint32_t delMask = (bitmask != nullptr) ? *(uint16_t*)(bitmask + (i >> 3)) : 0;
            uint32_t validMask = matchMask & (~delMask);
            
            while (validMask) {
                int tz = __builtin_ctz(validMask); 
                int rowId = i + tz;
                map->aggregate_single(keyData[rowId], valData[rowId]);
                matchCount++;
                validMask &= (validMask - 1); 
            }
        }
#endif

        // Scalar Tail
        for (; i < rows; i++) {
            bool match = false;
            if (opType == 0) match = (filterData[i] == targetVal);
            else if (opType == 1) match = (filterData[i] > targetVal);
            else if (opType == 2) match = (filterData[i] >= targetVal);
            else if (opType == 3) match = (filterData[i] < targetVal);
            else if (opType == 4) match = (filterData[i] <= targetVal);
            
            if (match) {
                bool isDeleted = (bitmask != nullptr) && ((bitmask[i >> 3] & (1 << (i & 7))) != 0);
                if (!isDeleted) {
                    map->aggregate_single(keyData[i], valData[i]);
                    matchCount++;
                }
            }
        }
        return matchCount;
    }

    // ==========================================================
    // MAP LIFECYCLE HOOKS
    // ==========================================================
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_aggregateCreateMapNative(
        JNIEnv* env, jobject obj, jint capacityHint
    ) {
        if (capacityHint <= 0) capacityHint = 1024;
        NativeHashMap* map = new (std::nothrow) NativeHashMap((size_t)capacityHint);
        return (jlong)map;
    }

    // ==========================================================
    // OPERATOR FUSION: Direct Aggregation (No Filter)
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxAggregateNative(
        JNIEnv* env, jobject obj, jlong blockPtr, 
        jint keyColIdx, jint valColIdx,
        jlong mapPtr, jlong deletedBitmaskPtr
    ) {
        if (blockPtr == 0 || mapPtr == 0) return 0;

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        
        int32_t* keyData = (int32_t*)(basePtr + colHeaders[keyColIdx].data_offset);
        int32_t* valData = (int32_t*)(basePtr + colHeaders[valColIdx].data_offset);
        
        NativeHashMap* map = (NativeHashMap*)mapPtr;
        uint8_t* bitmask = (uint8_t*)deletedBitmaskPtr;
        int rows = header->row_count;
        int aggregatedCount = 0;

        // Tight loop: Direct L1 Cache reads into the Hash Map
        for (int i = 0; i < rows; i++) {
            bool isDeleted = (bitmask != nullptr) && ((bitmask[i >> 3] & (1 << (i & 7))) != 0);
            if (!isDeleted) {
                map->aggregate_single(keyData[i], valData[i]);
                aggregatedCount++;
            }
        }
        return aggregatedCount;
    }
}