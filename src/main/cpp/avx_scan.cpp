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
}