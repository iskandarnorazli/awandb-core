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
#include "block.h" 
#include <immintrin.h>
#include <cstdio>

extern "C" {

    /*
     * [OPTIMIZED] AVX2 SCAN KERNEL
     * Features:
     * 1. Safety: Null pointer & Header checks.
     * 2. Speed: 8x Loop Unrolling (processes 64 integers per step).
     * 3. Latency: Uses hardware POPCNT for count-only queries.
     */
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint threshold, jlong outIndicesPtr
    ) {
        // --- 1. CRASH SHIELD (Safety) ---
        if (blockPtr == 0) return 0; 

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;

        if (colIdx < 0 || colIdx >= (int)header->column_count) return 0;

        // Skip Header to find Columns
        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];

        // Validate Data Pointer
        if (col.data_offset == 0) return 0; 
        
        // --- 2. SETUP POINTERS ---
        int32_t* data = (int32_t*)(basePtr + col.data_offset);
        int32_t* outIndices = (int32_t*)outIndicesPtr; // Can be NULL
        int rows = header->row_count;
        int matchCount = 0;

        // --- 3. EXECUTE OPTIMIZED SCAN ---
        if (col.type == TYPE_INT) {
            
            // Fast Path: Zone Map Pruning
            // If the whole block is smaller than threshold, skip it entirely.
            if (col.max_int <= threshold) return 0;
            
            // If the whole block is greater, return all rows (Count or Materialize)
            if (col.min_int > threshold) {
                if (outIndices != nullptr) {
                    for (int i = 0; i < rows; i++) outIndices[i] = i;
                }
                return rows;
            }

            __m256i vThresh = _mm256_set1_epi32(threshold);
            int i = 0;

            // --- MODE A: COUNT ONLY (Highest Speed) ---
            if (outIndices == nullptr) {
                // Unroll 64 items (8 Vectors) per iteration
                int limit = rows - 64;
                for (; i <= limit; i += 64) {
                    // 1. Load 8 vectors (Parallel Loads)
                    __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                    __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                    __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                    __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);
                    __m256i v4 = _mm256_loadu_si256((__m256i*)&data[i+32]);
                    __m256i v5 = _mm256_loadu_si256((__m256i*)&data[i+40]);
                    __m256i v6 = _mm256_loadu_si256((__m256i*)&data[i+48]);
                    __m256i v7 = _mm256_loadu_si256((__m256i*)&data[i+56]);

                    // 2. Compare (Parallel execution units)
                    __m256i m0 = _mm256_cmpgt_epi32(v0, vThresh);
                    __m256i m1 = _mm256_cmpgt_epi32(v1, vThresh);
                    __m256i m2 = _mm256_cmpgt_epi32(v2, vThresh);
                    __m256i m3 = _mm256_cmpgt_epi32(v3, vThresh);
                    __m256i m4 = _mm256_cmpgt_epi32(v4, vThresh);
                    __m256i m5 = _mm256_cmpgt_epi32(v5, vThresh);
                    __m256i m6 = _mm256_cmpgt_epi32(v6, vThresh);
                    __m256i m7 = _mm256_cmpgt_epi32(v7, vThresh);

                    // 3. Population Count (Hardware instruction, ~3 cycles)
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m0)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m1)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m2)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m3)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m4)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m5)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m6)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m7)));
                }
            } 
            // --- MODE B: MATERIALIZE INDICES (Standard Speed) ---
            else {
                // Unroll 32 items (4 Vectors) to balance Register Pressure vs Write Bandwidth
                int limit = rows - 32;
                for (; i <= limit; i += 32) {
                    // Prefetch next cache line
                    _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);

                    __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                    __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                    __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                    __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                    int mask0 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v0, vThresh)));
                    int mask1 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v1, vThresh)));
                    int mask2 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v2, vThresh)));
                    int mask3 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v3, vThresh)));

                    // Extract bits manually (faster than looping 0..8 if sparse)
                    if (mask0) { for(int k=0; k<8; k++) if((mask0>>k)&1) outIndices[matchCount++] = i+k; }
                    if (mask1) { for(int k=0; k<8; k++) if((mask1>>k)&1) outIndices[matchCount++] = i+8+k; }
                    if (mask2) { for(int k=0; k<8; k++) if((mask2>>k)&1) outIndices[matchCount++] = i+16+k; }
                    if (mask3) { for(int k=0; k<8; k++) if((mask3>>k)&1) outIndices[matchCount++] = i+24+k; }
                }
            }

            // --- 4. SCALAR TAIL (Handle remaining items) ---
            for (; i < rows; i++) {
                if (data[i] > threshold) {
                    if (outIndices != nullptr) {
                        outIndices[matchCount] = i;
                    }
                    matchCount++;
                }
            }
        }

        return matchCount;
    }

    // --- GATHER (Widening: Int -> Long) ---
    // out[i] = (long)base[indices[i]]
    // Fixes the corruption when copying 32-bit Integers into the 64-bit valuesPtr.
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_batchReadIntToLongNative(
        JNIEnv* env, jobject obj, jlong basePtr, jlong indicesPtr, jint count, jlong outPtr
    ) {
        if (!basePtr || !indicesPtr || !outPtr) return;
        
        int32_t* base = (int32_t*)basePtr;
        int32_t* indices = (int32_t*)indicesPtr;
        int64_t* out = (int64_t*)outPtr;
        
        for (int i = 0; i < count; i++) {
            out[i] = (int64_t)base[indices[i]];
        }
    }

    // ==========================================================
    // DIRTY SCAN: AVX2 WITH DELETION BITMASK PUSHDOWN
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockWithDeletionsNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint threshold, jlong deletedBitmaskPtr
    ) {
        if (blockPtr == 0) return 0;

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];
        
        if (col.data_offset == 0) return 0; 
        
        int32_t* data = (int32_t*)(basePtr + col.data_offset);
        uint8_t* bitmask = (uint8_t*)deletedBitmaskPtr; // 1 bit per row (1 = deleted)
        int rows = header->row_count;
        int matchCount = 0;

        if (col.type == TYPE_INT) {
            // Zone Map Pruning
            if (col.max_int <= threshold) return 0;

            __m256i vThresh = _mm256_set1_epi32(threshold);
            int i = 0;

#ifdef ARCH_X86
            // Process 32 rows at a time
            int limit = rows - 32;
            for (; i <= limit; i += 32) {
                __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                int m0 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v0, vThresh)));
                int m1 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v1, vThresh)));
                int m2 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v2, vThresh)));
                int m3 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v3, vThresh)));

                // Combine 4x 8-bit masks into one 32-bit mask representing matches
                uint32_t matchMask = (m0) | (m1 << 8) | (m2 << 16) | (m3 << 24);
                
                // Read 32 bits from the deletion bitmask
                uint32_t delMask = 0;
                if (bitmask != nullptr) {
                    // i is a multiple of 32, so (i >> 3) is always aligned to bytes
                    delMask = *(uint32_t*)(bitmask + (i >> 3));
                }

                // Keep only matches that are NOT deleted
                uint32_t validMask = matchMask & (~delMask);
                matchCount += _mm_popcnt_u32(validMask);
            }
#endif
            // Scalar Tail (or ARM Fallback)
            for (; i < rows; i++) {
                if (data[i] > threshold) {
                    bool isDeleted = false;
                    if (bitmask != nullptr) {
                        isDeleted = (bitmask[i >> 3] & (1 << (i & 7))) != 0;
                    }
                    if (!isDeleted) matchCount++;
                }
            }
        }
        return matchCount;
    }

    // ==========================================================
    // FAST RANDOM UPDATE (In-Place Memory Mutation)
    // ==========================================================
    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_updateCellNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint rowId, jint newValue
    ) {
        if (blockPtr == 0) return JNI_FALSE;
        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;
        
        if (colIdx < 0 || colIdx >= (int)header->column_count) return JNI_FALSE;
        if (rowId < 0 || rowId >= (int)header->row_count) return JNI_FALSE;

        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];
        if (col.data_offset == 0) return JNI_FALSE;

        if (col.type == TYPE_INT) {
            int32_t* data = (int32_t*)(basePtr + col.data_offset);
            data[rowId] = newValue;
            
            // Expand Zone Map if necessary
            if (newValue < col.min_int) col.min_int = newValue;
            if (newValue > col.max_int) col.max_int = newValue;

            return JNI_TRUE;
        }
        return JNI_FALSE;
    }

    // ==========================================================
    // PREDICATE PUSHDOWN: GENERIC AVX2 FILTER
    // opType -> 0: ==, 1: >, 2: >=, 3: <, 4: <=
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxFilterBlockNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint opType, jint targetVal, jlong outIndicesPtr, jlong deletedBitmaskPtr
    ) {
        if (blockPtr == 0 || outIndicesPtr == 0) return 0;

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];
        
        if (col.data_offset == 0) return 0; 
        
        int32_t* data = (int32_t*)(basePtr + col.data_offset);
        int32_t* outIndices = (int32_t*)outIndicesPtr;
        uint8_t* bitmask = (uint8_t*)deletedBitmaskPtr;
        int rows = header->row_count;
        int matchCount = 0;

        if (col.type == TYPE_INT) {
            // Zone Map Pruning (Instant Rejection)
            if (opType == 0 && (targetVal < col.min_int || targetVal > col.max_int)) return 0; 
            if (opType == 1 && col.max_int <= targetVal) return 0; 
            if (opType == 2 && col.max_int < targetVal) return 0; 
            if (opType == 3 && col.min_int >= targetVal) return 0; 
            if (opType == 4 && col.min_int > targetVal) return 0; 

            __m256i vTarget = _mm256_set1_epi32(targetVal);
            int i = 0;

#ifdef ARCH_X86
            int limit = rows - 32;
            for (; i <= limit; i += 32) {
                __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                __m256i cmp0, cmp1, cmp2, cmp3;
                if (opType == 0) { // EQ
                    cmp0 = _mm256_cmpeq_epi32(v0, vTarget);
                    cmp1 = _mm256_cmpeq_epi32(v1, vTarget);
                    cmp2 = _mm256_cmpeq_epi32(v2, vTarget);
                    cmp3 = _mm256_cmpeq_epi32(v3, vTarget);
                } else if (opType == 1) { // GT
                    cmp0 = _mm256_cmpgt_epi32(v0, vTarget);
                    cmp1 = _mm256_cmpgt_epi32(v1, vTarget);
                    cmp2 = _mm256_cmpgt_epi32(v2, vTarget);
                    cmp3 = _mm256_cmpgt_epi32(v3, vTarget);
                } else if (opType == 2) { // GTE (v0 > target - 1)
                    __m256i vT = _mm256_set1_epi32(targetVal - 1);
                    cmp0 = _mm256_cmpgt_epi32(v0, vT);
                    cmp1 = _mm256_cmpgt_epi32(v1, vT);
                    cmp2 = _mm256_cmpgt_epi32(v2, vT);
                    cmp3 = _mm256_cmpgt_epi32(v3, vT);
                } else if (opType == 3) { // LT (target > v0)
                    cmp0 = _mm256_cmpgt_epi32(vTarget, v0);
                    cmp1 = _mm256_cmpgt_epi32(vTarget, v1);
                    cmp2 = _mm256_cmpgt_epi32(vTarget, v2);
                    cmp3 = _mm256_cmpgt_epi32(vTarget, v3);
                } else { // LTE (~(v0 > target))
                    cmp0 = _mm256_cmpeq_epi32(_mm256_cmpgt_epi32(v0, vTarget), _mm256_setzero_si256());
                    cmp1 = _mm256_cmpeq_epi32(_mm256_cmpgt_epi32(v1, vTarget), _mm256_setzero_si256());
                    cmp2 = _mm256_cmpeq_epi32(_mm256_cmpgt_epi32(v2, vTarget), _mm256_setzero_si256());
                    cmp3 = _mm256_cmpeq_epi32(_mm256_cmpgt_epi32(v3, vTarget), _mm256_setzero_si256());
                }

                int m0 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp0));
                int m1 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp1));
                int m2 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp2));
                int m3 = _mm256_movemask_ps(_mm256_castsi256_ps(cmp3));

                uint32_t matchMask = (m0) | (m1 << 8) | (m2 << 16) | (m3 << 24);
                uint32_t delMask = (bitmask != nullptr) ? *(uint32_t*)(bitmask + (i >> 3)) : 0;
                uint32_t validMask = matchMask & (~delMask);
                
                // Fast extraction of matching indices
                while (validMask) {
                    unsigned long tz;
#ifdef _MSC_VER
                    // MSVC (Windows) implementation
                    _BitScanForward(&tz, validMask);
#else
                    // GCC/Clang (Linux/Mac) implementation
                    tz = __builtin_ctz(validMask); 
#endif
                    outIndices[matchCount++] = i + tz;
                    validMask &= (validMask - 1);      // Clear lowest set bit
                }
            }
#endif
            for (; i < rows; i++) {
                bool match = false;
                if (opType == 0) match = (data[i] == targetVal);
                else if (opType == 1) match = (data[i] > targetVal);
                else if (opType == 2) match = (data[i] >= targetVal);
                else if (opType == 3) match = (data[i] < targetVal);
                else if (opType == 4) match = (data[i] <= targetVal);

                if (match) {
                    bool isDeleted = (bitmask != nullptr) && ((bitmask[i >> 3] & (1 << (i & 7))) != 0);
                    if (!isDeleted) outIndices[matchCount++] = i;
                }
            }
        }
        return matchCount;
    }
}