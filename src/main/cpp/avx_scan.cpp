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

#ifdef ARCH_X86
    #include <immintrin.h>
#elif defined(ARCH_ARM)
    #include <arm_neon.h>
#endif

#include <cstdio>

// Helper to extract a 4-bit mask from a 128-bit NEON vector of 32-bit integers
#ifdef ARCH_ARM
inline int neon_movemask_epi32(uint32x4_t cmp) {
    return (vgetq_lane_u32(cmp, 0) & 1) |
          ((vgetq_lane_u32(cmp, 1) & 1) << 1) |
          ((vgetq_lane_u32(cmp, 2) & 1) << 2) |
          ((vgetq_lane_u32(cmp, 3) & 1) << 3);
}
#endif

extern "C" {

    /*
     * [OPTIMIZED] HYBRID SCAN KERNEL (AVX2 + NEON)
     */
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint threshold, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0; 

        uint8_t* basePtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)basePtr;

        if (colIdx < 0 || colIdx >= (int)header->column_count) return 0;

        ColumnHeader* colHeaders = (ColumnHeader*)(basePtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];

        if (col.data_offset == 0) return 0; 
        
        int32_t* data = (int32_t*)(basePtr + col.data_offset);
        int32_t* outIndices = (int32_t*)outIndicesPtr;
        int rows = header->row_count;
        int matchCount = 0;

        if (col.type == TYPE_INT) {
            
            // Fast Path: Zone Map Pruning
            if (col.max_int <= threshold) return 0;
            if (col.min_int > threshold) {
                if (outIndices != nullptr) {
                    for (int i = 0; i < rows; i++) outIndices[i] = i;
                }
                return rows;
            }

            int i = 0;

#ifdef ARCH_X86
            __m256i vThresh = _mm256_set1_epi32(threshold);
            
            if (outIndices == nullptr) {
                int limit = rows - 64;
                for (; i <= limit; i += 64) {
                    __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                    __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                    __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                    __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);
                    __m256i v4 = _mm256_loadu_si256((__m256i*)&data[i+32]);
                    __m256i v5 = _mm256_loadu_si256((__m256i*)&data[i+40]);
                    __m256i v6 = _mm256_loadu_si256((__m256i*)&data[i+48]);
                    __m256i v7 = _mm256_loadu_si256((__m256i*)&data[i+56]);

                    __m256i m0 = _mm256_cmpgt_epi32(v0, vThresh);
                    __m256i m1 = _mm256_cmpgt_epi32(v1, vThresh);
                    __m256i m2 = _mm256_cmpgt_epi32(v2, vThresh);
                    __m256i m3 = _mm256_cmpgt_epi32(v3, vThresh);
                    __m256i m4 = _mm256_cmpgt_epi32(v4, vThresh);
                    __m256i m5 = _mm256_cmpgt_epi32(v5, vThresh);
                    __m256i m6 = _mm256_cmpgt_epi32(v6, vThresh);
                    __m256i m7 = _mm256_cmpgt_epi32(v7, vThresh);

                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m0)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m1)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m2)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m3)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m4)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m5)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m6)));
                    matchCount += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(m7)));
                }
            } else {
                int limit = rows - 32;
                for (; i <= limit; i += 32) {
                    _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);

                    __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                    __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                    __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                    __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                    int mask0 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v0, vThresh)));
                    int mask1 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v1, vThresh)));
                    int mask2 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v2, vThresh)));
                    int mask3 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v3, vThresh)));

                    if (mask0) { for(int k=0; k<8; k++) if((mask0>>k)&1) outIndices[matchCount++] = i+k; }
                    if (mask1) { for(int k=0; k<8; k++) if((mask1>>k)&1) outIndices[matchCount++] = i+8+k; }
                    if (mask2) { for(int k=0; k<8; k++) if((mask2>>k)&1) outIndices[matchCount++] = i+16+k; }
                    if (mask3) { for(int k=0; k<8; k++) if((mask3>>k)&1) outIndices[matchCount++] = i+24+k; }
                }
            }
#elif defined(ARCH_ARM)
            int32x4_t vThresh = vdupq_n_s32(threshold);
            
            if (outIndices == nullptr) {
                int limit = rows - 32;
                for (; i <= limit; i += 32) {
                    int32x4_t v0 = vld1q_s32(&data[i]);
                    int32x4_t v1 = vld1q_s32(&data[i+4]);
                    int32x4_t v2 = vld1q_s32(&data[i+8]);
                    int32x4_t v3 = vld1q_s32(&data[i+12]);
                    int32x4_t v4 = vld1q_s32(&data[i+16]);
                    int32x4_t v5 = vld1q_s32(&data[i+20]);
                    int32x4_t v6 = vld1q_s32(&data[i+24]);
                    int32x4_t v7 = vld1q_s32(&data[i+28]);

                    uint32x4_t m0 = vcgtq_s32(v0, vThresh);
                    uint32x4_t m1 = vcgtq_s32(v1, vThresh);
                    uint32x4_t m2 = vcgtq_s32(v2, vThresh);
                    uint32x4_t m3 = vcgtq_s32(v3, vThresh);
                    uint32x4_t m4 = vcgtq_s32(v4, vThresh);
                    uint32x4_t m5 = vcgtq_s32(v5, vThresh);
                    uint32x4_t m6 = vcgtq_s32(v6, vThresh);
                    uint32x4_t m7 = vcgtq_s32(v7, vThresh);

                    int32x4_t sum1 = vaddq_s32((int32x4_t)m0, (int32x4_t)m1);
                    sum1 = vaddq_s32(sum1, (int32x4_t)m2);
                    sum1 = vaddq_s32(sum1, (int32x4_t)m3);

                    int32x4_t sum2 = vaddq_s32((int32x4_t)m4, (int32x4_t)m5);
                    sum2 = vaddq_s32(sum2, (int32x4_t)m6);
                    sum2 = vaddq_s32(sum2, (int32x4_t)m7);

                    matchCount -= (vgetq_lane_s32(sum1, 0) + vgetq_lane_s32(sum1, 1) + 
                                   vgetq_lane_s32(sum1, 2) + vgetq_lane_s32(sum1, 3));
                    matchCount -= (vgetq_lane_s32(sum2, 0) + vgetq_lane_s32(sum2, 1) + 
                                   vgetq_lane_s32(sum2, 2) + vgetq_lane_s32(sum2, 3));
                }
            } else {
                int limit = rows - 16;
                for (; i <= limit; i += 16) {
                    int32x4_t v0 = vld1q_s32(&data[i]);
                    int32x4_t v1 = vld1q_s32(&data[i+4]);
                    int32x4_t v2 = vld1q_s32(&data[i+8]);
                    int32x4_t v3 = vld1q_s32(&data[i+12]);

                    int mask0 = neon_movemask_epi32(vcgtq_s32(v0, vThresh));
                    int mask1 = neon_movemask_epi32(vcgtq_s32(v1, vThresh));
                    int mask2 = neon_movemask_epi32(vcgtq_s32(v2, vThresh));
                    int mask3 = neon_movemask_epi32(vcgtq_s32(v3, vThresh));

                    if (mask0) { for(int k=0; k<4; k++) if((mask0>>k)&1) outIndices[matchCount++] = i+k; }
                    if (mask1) { for(int k=0; k<4; k++) if((mask1>>k)&1) outIndices[matchCount++] = i+4+k; }
                    if (mask2) { for(int k=0; k<4; k++) if((mask2>>k)&1) outIndices[matchCount++] = i+8+k; }
                    if (mask3) { for(int k=0; k<4; k++) if((mask3>>k)&1) outIndices[matchCount++] = i+12+k; }
                }
            }
#endif

            // Scalar Tail
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
    // DIRTY SCAN: HYBRID AVX2/NEON WITH DELETION BITMASK PUSHDOWN
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
        uint8_t* bitmask = (uint8_t*)deletedBitmaskPtr; 
        int rows = header->row_count;
        int matchCount = 0;

        if (col.type == TYPE_INT) {
            if (col.max_int <= threshold) return 0;
            int i = 0;

#ifdef ARCH_X86
            __m256i vThresh = _mm256_set1_epi32(threshold);
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

                uint32_t matchMask = (m0) | (m1 << 8) | (m2 << 16) | (m3 << 24);
                uint32_t delMask = (bitmask != nullptr) ? *(uint32_t*)(bitmask + (i >> 3)) : 0;
                uint32_t validMask = matchMask & (~delMask);
                matchCount += _mm_popcnt_u32(validMask);
            }
#elif defined(ARCH_ARM)
            int32x4_t vThresh = vdupq_n_s32(threshold);
            int limit = rows - 16;
            for (; i <= limit; i += 16) {
                int32x4_t v0 = vld1q_s32(&data[i]);
                int32x4_t v1 = vld1q_s32(&data[i+4]);
                int32x4_t v2 = vld1q_s32(&data[i+8]);
                int32x4_t v3 = vld1q_s32(&data[i+12]);

                int m0 = neon_movemask_epi32(vcgtq_s32(v0, vThresh));
                int m1 = neon_movemask_epi32(vcgtq_s32(v1, vThresh));
                int m2 = neon_movemask_epi32(vcgtq_s32(v2, vThresh));
                int m3 = neon_movemask_epi32(vcgtq_s32(v3, vThresh));

                uint32_t matchMask = (m0) | (m1 << 4) | (m2 << 8) | (m3 << 12);
                uint32_t delMask = (bitmask != nullptr) ? *(uint16_t*)(bitmask + (i >> 3)) : 0;
                uint32_t validMask = matchMask & (~delMask);
                matchCount += __builtin_popcount(validMask);
            }
#endif
            // Scalar Tail
            for (; i < rows; i++) {
                if (data[i] > threshold) {
                    bool isDeleted = (bitmask != nullptr) && ((bitmask[i >> 3] & (1 << (i & 7))) != 0);
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
            
            if (newValue < col.min_int) col.min_int = newValue;
            if (newValue > col.max_int) col.max_int = newValue;

            return JNI_TRUE;
        }
        return JNI_FALSE;
    }

    // ==========================================================
    // PREDICATE PUSHDOWN: HYBRID AVX2/NEON FILTER
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
            if (opType == 0 && (targetVal < col.min_int || targetVal > col.max_int)) return 0; 
            if (opType == 1 && col.max_int <= targetVal) return 0; 
            if (opType == 2 && col.max_int < targetVal) return 0; 
            if (opType == 3 && col.min_int >= targetVal) return 0; 
            if (opType == 4 && col.min_int > targetVal) return 0; 

            int i = 0;

#ifdef ARCH_X86
            __m256i vTarget = _mm256_set1_epi32(targetVal);
            int limit = rows - 32;
            for (; i <= limit; i += 32) {
                __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                __m256i cmp0, cmp1, cmp2, cmp3;
                if (opType == 0) {
                    cmp0 = _mm256_cmpeq_epi32(v0, vTarget);
                    cmp1 = _mm256_cmpeq_epi32(v1, vTarget);
                    cmp2 = _mm256_cmpeq_epi32(v2, vTarget);
                    cmp3 = _mm256_cmpeq_epi32(v3, vTarget);
                } else if (opType == 1) { 
                    cmp0 = _mm256_cmpgt_epi32(v0, vTarget);
                    cmp1 = _mm256_cmpgt_epi32(v1, vTarget);
                    cmp2 = _mm256_cmpgt_epi32(v2, vTarget);
                    cmp3 = _mm256_cmpgt_epi32(v3, vTarget);
                } else if (opType == 2) { 
                    __m256i vT = _mm256_set1_epi32(targetVal - 1);
                    cmp0 = _mm256_cmpgt_epi32(v0, vT);
                    cmp1 = _mm256_cmpgt_epi32(v1, vT);
                    cmp2 = _mm256_cmpgt_epi32(v2, vT);
                    cmp3 = _mm256_cmpgt_epi32(v3, vT);
                } else if (opType == 3) { 
                    cmp0 = _mm256_cmpgt_epi32(vTarget, v0);
                    cmp1 = _mm256_cmpgt_epi32(vTarget, v1);
                    cmp2 = _mm256_cmpgt_epi32(vTarget, v2);
                    cmp3 = _mm256_cmpgt_epi32(vTarget, v3);
                } else { 
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
                
                while (validMask) {
                    unsigned long tz;
#ifdef _MSC_VER
                    _BitScanForward(&tz, validMask);
#else
                    tz = __builtin_ctz(validMask); 
#endif
                    outIndices[matchCount++] = i + tz;
                    validMask &= (validMask - 1); 
                }
            }
#elif defined(ARCH_ARM)
            int32x4_t vTarget = vdupq_n_s32(targetVal);
            int limit = rows - 16;
            for (; i <= limit; i += 16) {
                int32x4_t v0 = vld1q_s32(&data[i]);
                int32x4_t v1 = vld1q_s32(&data[i+4]);
                int32x4_t v2 = vld1q_s32(&data[i+8]);
                int32x4_t v3 = vld1q_s32(&data[i+12]);

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

                int m0 = neon_movemask_epi32(cmp0);
                int m1 = neon_movemask_epi32(cmp1);
                int m2 = neon_movemask_epi32(cmp2);
                int m3 = neon_movemask_epi32(cmp3);

                uint32_t matchMask = (m0) | (m1 << 4) | (m2 << 8) | (m3 << 12);
                uint32_t delMask = (bitmask != nullptr) ? *(uint16_t*)(bitmask + (i >> 3)) : 0;
                uint32_t validMask = matchMask & (~delMask);
                
                while (validMask) {
                    int tz = __builtin_ctz(validMask); 
                    outIndices[matchCount++] = i + tz;
                    validMask &= (validMask - 1); 
                }
            }
#endif

            // Scalar Tail
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