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
#include <limits>

// --- ARCHITECTURE SPECIFIC HEADERS ---
#ifdef ARCH_X86
    #include <immintrin.h>
    #include <nmmintrin.h> // For CRC32 (SSE4.2)
#endif

#ifdef ARCH_ARM
    #include <arm_neon.h>
    #include <arm_acle.h>  // For CRC32 intrinsics (__crc32d)
#endif

extern "C" {

    /* [Dead code removed for clarity] */

    // ==========================================================
    // 1. MULTI-BLOCK SCAN (Hybrid AVX2/NEON)
    // ==========================================================
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanMultiBlockNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jintArray jThresholds, jintArray jCounts
    ) {
        if (blockPtr == 0) return;
        
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        
        int32_t minVal = colHeaders[colIdx].min_int;
        int32_t maxVal = colHeaders[colIdx].max_int;
        int32_t rows = blkHeader->row_count;
        
        jint* thresholds = (jint*)env->GetPrimitiveArrayCritical(jThresholds, nullptr);
        jint* counts = (jint*)env->GetPrimitiveArrayCritical(jCounts, nullptr);
        jsize numQueries = env->GetArrayLength(jThresholds);

        // Metadata Pruning
        int32_t minQuery = std::numeric_limits<int32_t>::max();
        int32_t maxQuery = std::numeric_limits<int32_t>::min();
        for (int q = 0; q < numQueries; q++) {
            if (thresholds[q] < minQuery) minQuery = thresholds[q];
            if (thresholds[q] > maxQuery) maxQuery = thresholds[q];
        }

        if (maxVal <= minQuery) {
            env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
            env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
            return;
        }
        if (minVal > maxQuery) {
            for (int q = 0; q < numQueries; q++) counts[q] += rows; 
            env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
            env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
            return;
        }

        int* data = (int*)(rawPtr + colHeaders[colIdx].data_offset);
        int i = 0;

        // --- PATH A: AVX2 (Intel/AMD) ---
#ifdef ARCH_X86
        int limit = rows - 32; // Changed from size_t
        for (; i <= limit; i += 32) { // Changed < to <= to process the final full chunk
            _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);
            _mm_prefetch((const char*)&data[i + 48], _MM_HINT_T0);
            
            __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
            __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
            __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);
            
            for (int q = 0; q < numQueries; q++) {
                __m256i vThresh = _mm256_set1_epi32(thresholds[q]);
                int c = 0;
                c += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v0, vThresh))));
                c += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v1, vThresh))));
                c += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v2, vThresh))));
                c += _mm_popcnt_u32(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v3, vThresh))));
                counts[q] += c;
            }
        }
        
        // --- PATH B: NEON (ARM/Apple) ---
#elif defined(ARCH_ARM)
        int limit = rows - 16; // Changed from size_t
        for (; i <= limit; i += 16) { // Changed < to <= 
            int32x4_t v0 = vld1q_s32(&data[i]);
            int32x4_t v1 = vld1q_s32(&data[i+4]);
            int32x4_t v2 = vld1q_s32(&data[i+8]);
            int32x4_t v3 = vld1q_s32(&data[i+12]);

            for (int q = 0; q < numQueries; q++) {
                int32x4_t vThresh = vdupq_n_s32(thresholds[q]);
                
                // Compare (True = -1, False = 0)
                uint32x4_t m0 = vcgtq_s32(v0, vThresh);
                uint32x4_t m1 = vcgtq_s32(v1, vThresh);
                uint32x4_t m2 = vcgtq_s32(v2, vThresh);
                uint32x4_t m3 = vcgtq_s32(v3, vThresh);

                // Add vertically (-1 adds up to negative count)
                int32x4_t sum = vaddq_s32((int32x4_t)m0, (int32x4_t)m1);
                sum = vaddq_s32(sum, (int32x4_t)m2);
                sum = vaddq_s32(sum, (int32x4_t)m3);

                // Horizontal add and negate
                int c = -(vgetq_lane_s32(sum, 0) + vgetq_lane_s32(sum, 1) + 
                          vgetq_lane_s32(sum, 2) + vgetq_lane_s32(sum, 3));
                counts[q] += c;
            }
        }
#endif

        // Scalar Tail
        for (; i < (size_t)rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) counts[q]++;
            }
        }
        
        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    // ==========================================================
    // 2. STRING SCAN (Hybrid AVX2/NEON)
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanStringNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jstring search, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        GermanString* data = (GermanString*)(rawPtr + ch->data_offset);
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        int rows = (int)(ch->data_length / 16); 
        
        const char* cstr = env->GetStringUTFChars(search, nullptr);
        uint32_t sLen = (uint32_t)strlen(cstr);
        GermanString target(cstr, sLen, nullptr); 

        // Cross-Platform SIMD Loader (Defined in german_string.h)
#ifdef ARCH_X86
        __m128i vTarget = target.toSIMD(); 
#elif defined(ARCH_ARM)
        uint8x16_t vTarget = target.toSIMD();
#endif

        int requiredMask = (sLen <= 12) ? 0xFFFF : 0x00FF;
        int i = 0;
        int limit = rows - 4;

#ifdef ARCH_X86
        if (outIndicesPtr == 0) {
            for (; i <= limit; i += 4) {
                __m128i v0 = _mm_loadu_si128((__m128i*)&data[i]);
                __m128i v1 = _mm_loadu_si128((__m128i*)&data[i+1]);
                __m128i v2 = _mm_loadu_si128((__m128i*)&data[i+2]);
                __m128i v3 = _mm_loadu_si128((__m128i*)&data[i+3]);
                
                int m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(v0, vTarget));
                int m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, vTarget));
                int m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(v2, vTarget));
                int m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(v3, vTarget));
                
                if ((m0 & requiredMask) == requiredMask && data[i].equals(target)) matchCount++;
                if ((m1 & requiredMask) == requiredMask && data[i+1].equals(target)) matchCount++;
                if ((m2 & requiredMask) == requiredMask && data[i+2].equals(target)) matchCount++;
                if ((m3 & requiredMask) == requiredMask && data[i+3].equals(target)) matchCount++;
            }
        } else {
             // Materialize Indices Path
             for (; i <= limit; i += 4) {
                __m128i v0 = _mm_loadu_si128((__m128i*)&data[i]);
                __m128i v1 = _mm_loadu_si128((__m128i*)&data[i+1]);
                __m128i v2 = _mm_loadu_si128((__m128i*)&data[i+2]);
                __m128i v3 = _mm_loadu_si128((__m128i*)&data[i+3]);
                
                int m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(v0, vTarget));
                int m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, vTarget));
                int m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(v2, vTarget));
                int m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(v3, vTarget));
                
                if ((m0 & requiredMask) == requiredMask && data[i].equals(target)) out[matchCount++] = i;
                if ((m1 & requiredMask) == requiredMask && data[i+1].equals(target)) out[matchCount++] = i+1;
                if ((m2 & requiredMask) == requiredMask && data[i+2].equals(target)) out[matchCount++] = i+2;
                if ((m3 & requiredMask) == requiredMask && data[i+3].equals(target)) out[matchCount++] = i+3;
             }
        }
#elif defined(ARCH_ARM)
        // NEON Path using common.h movemask helper
        if (outIndicesPtr == 0) {
            for (; i <= limit; i += 4) {
                uint8x16_t v0 = vld1q_u8((const uint8_t*)&data[i]);
                uint8x16_t v1 = vld1q_u8((const uint8_t*)&data[i+1]);
                uint8x16_t v2 = vld1q_u8((const uint8_t*)&data[i+2]);
                uint8x16_t v3 = vld1q_u8((const uint8_t*)&data[i+3]);

                uint8x16_t c0 = vceqq_u8(v0, vTarget);
                uint8x16_t c1 = vceqq_u8(v1, vTarget);
                uint8x16_t c2 = vceqq_u8(v2, vTarget);
                uint8x16_t c3 = vceqq_u8(v3, vTarget);

                int m0 = neon_movemask_u8(c0);
                int m1 = neon_movemask_u8(c1);
                int m2 = neon_movemask_u8(c2);
                int m3 = neon_movemask_u8(c3);

                if ((m0 & requiredMask) == requiredMask && data[i].equals(target)) matchCount++;
                if ((m1 & requiredMask) == requiredMask && data[i+1].equals(target)) matchCount++;
                if ((m2 & requiredMask) == requiredMask && data[i+2].equals(target)) matchCount++;
                if ((m3 & requiredMask) == requiredMask && data[i+3].equals(target)) matchCount++;
            }
        }
        // ... (Materialize path omitted, scalar tail handles it)
#endif

        // Scalar Tail
        for (; i < rows; i++) {
            if (data[i].equals(target)) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }
        
        env->ReleaseStringUTFChars(search, cstr);
        return matchCount;
    }

    // --- UTILS ---

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_batchReadNative(
        JNIEnv* env, jobject obj, jlong colPtr, jlong indicesPtr, jint count, jlong outDataPtr
    ) {
        if (colPtr == 0 || indicesPtr == 0 || outDataPtr == 0) return;
        int* data = (int*)colPtr;
        int* indices = (int*)indicesPtr;
        int* result = (int*)outDataPtr;
        for (int i = 0; i < count; i++) { result[i] = data[indices[i]]; }
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanIndicesNative(JNIEnv* env, jobject obj, jlong colPtr, jint rows, jint threshold, jlong outIndicesPtr) {
        if (colPtr == 0) return 0;
        int* data = (int*)colPtr;
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        for (int i = 0; i < rows; i++) {
            if (data[i] > threshold) out[matchCount++] = i;
        }
        return matchCount;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanIndicesMultiNative(JNIEnv* env, jobject obj, jlong colPtr, jint rows, jintArray jThresholds, jintArray jCounts) {
         if (colPtr == 0) return;
         int* data = (int*)colPtr;
         jint* thresholds = (jint*)env->GetPrimitiveArrayCritical(jThresholds, nullptr);
         jint* counts = (jint*)env->GetPrimitiveArrayCritical(jCounts, nullptr);
         jsize numQueries = env->GetArrayLength(jThresholds);
         std::memset(counts, 0, (size_t)numQueries * sizeof(int));
         for (int i = 0; i < rows; i++) {
             int val = data[i];
             for (int q = 0; q < numQueries; q++) {
                 if (val > thresholds[q]) counts[q]++;
             }
         }
         env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
         env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanArrayNative(JNIEnv* env, jobject obj, jintArray jData, jint threshold) {
        if (jData == NULL) return 0;
        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jsize rows = env->GetArrayLength(jData);
        int matchCount = 0;
        int i = 0;

#ifdef ARCH_X86
        size_t alignedLimit = (size_t)rows - (rows % 8);
        __m256i thresholdVec = _mm256_set1_epi32(threshold);
        for (; i < alignedLimit; i += 8) {
            __m256i valVec = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i maskVec = _mm256_cmpgt_epi32(valVec, thresholdVec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(maskVec));
            if (mask != 0) matchCount += _mm_popcnt_u32(mask);
        }
#elif defined(ARCH_ARM)
        size_t alignedLimit = (size_t)rows - (rows % 16);
        int32x4_t vThresh = vdupq_n_s32(threshold);
        for (; i < alignedLimit; i += 16) {
             int32x4_t v0 = vld1q_s32(&data[i]);
             int32x4_t v1 = vld1q_s32(&data[i+4]);
             int32x4_t v2 = vld1q_s32(&data[i+8]);
             int32x4_t v3 = vld1q_s32(&data[i+12]);
             
             uint32x4_t m0 = vcgtq_s32(v0, vThresh);
             uint32x4_t m1 = vcgtq_s32(v1, vThresh);
             uint32x4_t m2 = vcgtq_s32(v2, vThresh);
             uint32x4_t m3 = vcgtq_s32(v3, vThresh);
             
             int32x4_t sum = vaddq_s32((int32x4_t)m0, (int32x4_t)m1);
             sum = vaddq_s32(sum, (int32x4_t)m2);
             sum = vaddq_s32(sum, (int32x4_t)m3);
             
             matchCount -= (vgetq_lane_s32(sum, 0) + vgetq_lane_s32(sum, 1) + 
                            vgetq_lane_s32(sum, 2) + vgetq_lane_s32(sum, 3));
        }
#endif
        for (; i < (size_t)rows; i++) {
            if (data[i] > threshold) matchCount++;
        }
        env->ReleasePrimitiveArrayCritical(jData, data, 0);
        return matchCount;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanArrayMultiNative(JNIEnv* env, jobject obj, jintArray jData, jintArray jThresholds, jintArray jCounts) {
        if (jData == NULL) return;
        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jint* thresholds = (jint*)env->GetPrimitiveArrayCritical(jThresholds, nullptr);
        jint* counts = (jint*)env->GetPrimitiveArrayCritical(jCounts, nullptr);
        jsize rows = env->GetArrayLength(jData);
        jsize numQueries = env->GetArrayLength(jThresholds);
        size_t i = 0;
        
        // Scalar Fallback
        for (; i < (size_t)rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) counts[q]++;
            }
        }
        env->ReleasePrimitiveArrayCritical(jData, data, 0);
        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    // ==========================================================
    // 3. VECTOR OPS (Cosine Sim) - Hybrid AVX2/NEON
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanVectorCosineNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jfloatArray jQuery, jfloat threshold, jlong outIndicesPtr) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        if (ch->type != TYPE_VECTOR) return 0;
        int dim = (int)ch->vector_dim;
        int rows = (int)(ch->data_length / ch->stride); 
        float* data = (float*)(rawPtr + ch->data_offset);
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        jfloat* query = env->GetFloatArrayElements(jQuery, nullptr);
        int dimLimit = dim - (dim % 8); 

        for (int i = 0; i < rows; i++) {
            float* vec = data + (i * dim);
            
#ifdef ARCH_X86
            __m256 sum = _mm256_setzero_ps();
            for (int d = 0; d < dimLimit; d += 8) {
                __m256 vA = _mm256_loadu_ps(&vec[d]);
                __m256 vB = _mm256_loadu_ps(&query[d]);
                sum = _mm256_fmadd_ps(vA, vB, sum);
            }
            __m256 shuf = _mm256_permute2f128_ps(sum, sum, 1);
            sum = _mm256_add_ps(sum, shuf);
            sum = _mm256_hadd_ps(sum, sum); 
            sum = _mm256_hadd_ps(sum, sum); 
            float score = _mm256_cvtss_f32(sum);
#elif defined(ARCH_ARM)
            float32x4_t sumVec = vdupq_n_f32(0.0f);
            int neonLimit = dim - (dim % 4);
            for (int d = 0; d < neonLimit; d += 4) {
                 float32x4_t vA = vld1q_f32(&vec[d]);
                 float32x4_t vB = vld1q_f32(&query[d]);
                 sumVec = vmlaq_f32(sumVec, vA, vB); // Fused Multiply-Add
            }
            float score = vgetq_lane_f32(sumVec, 0) + vgetq_lane_f32(sumVec, 1) + 
                          vgetq_lane_f32(sumVec, 2) + vgetq_lane_f32(sumVec, 3);
            dimLimit = neonLimit; 
#else
            float score = 0.0f;
            dimLimit = 0;
#endif
            for (int d = dimLimit; d < dim; d++) {
                score += vec[d] * query[d];
            }
            if (score >= threshold) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }
        env->ReleaseFloatArrayElements(jQuery, query, 0);
        return matchCount;
    }

    // ==========================================================
    // 4. VECTOR HASHING (CRC32) - [FIXED]
    // ==========================================================
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxHashVectorNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jlong outHashPtr) {
        if (blockPtr == 0 || outHashPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        
        if (ch->type != TYPE_VECTOR) return;

        int stride = ch->stride;
        int rows = (int)(ch->data_length / stride);
        uint8_t* data = (uint8_t*)(rawPtr + ch->data_offset);
        int64_t* out = (int64_t*)outHashPtr; 

        for (int i = 0; i < rows; i++) {
            uint8_t* item = data + (i * stride);
            uint64_t hash = 0;
            int len = stride;
            uint64_t* ptr64 = (uint64_t*)item;
            
            while (len >= 8) {
#ifdef ARCH_X86
                hash = _mm_crc32_u64(hash, *ptr64++);
#elif defined(ARCH_ARM)
                // Requires __crc32d intrinsic (ARMv8-A + CRC)
                hash = __crc32d(hash, *ptr64++);
#else
                // Simple Fallback Mixer
                uint64_t k = *ptr64++;
                k ^= k >> 33; k *= 0xff51afd7ed558ccd; k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53; k ^= k >> 33;
                hash ^= k;
#endif
                len -= 8;
            }
            
            if (len > 0) {
                uint8_t* ptr8 = (uint8_t*)ptr64;
                while (len > 0) {
#ifdef ARCH_X86
                    hash = _mm_crc32_u8((uint32_t)hash, *ptr8++);
#elif defined(ARCH_ARM)
                    hash = __crc32b((uint32_t)hash, *ptr8++);
#else 
                    hash = (hash * 33) ^ *ptr8++;
#endif
                    len--;
                }
            }
            out[i] = (int64_t)hash;
        }
    }

    // ==========================================================
    // 5. EQUALITY SCAN (Hybrid AVX2/NEON)
    // ==========================================================
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockEqualityNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint targetVal, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* header = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        ColumnHeader& col = colHeaders[colIdx];

        if (targetVal < col.min_int || targetVal > col.max_int) return 0;

        int32_t* data = (int32_t*)(rawPtr + col.data_offset);
        int32_t* out = (int32_t*)outIndicesPtr;
        int rows = header->row_count;
        int matchCount = 0;
        int i = 0;

#ifdef ARCH_X86
        __m256i vTarget = _mm256_set1_epi32(targetVal);
        if (outIndicesPtr == 0) {
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

                __m256i m0 = _mm256_cmpeq_epi32(v0, vTarget);
                __m256i m1 = _mm256_cmpeq_epi32(v1, vTarget);
                __m256i m2 = _mm256_cmpeq_epi32(v2, vTarget);
                __m256i m3 = _mm256_cmpeq_epi32(v3, vTarget);
                __m256i m4 = _mm256_cmpeq_epi32(v4, vTarget);
                __m256i m5 = _mm256_cmpeq_epi32(v5, vTarget);
                __m256i m6 = _mm256_cmpeq_epi32(v6, vTarget);
                __m256i m7 = _mm256_cmpeq_epi32(v7, vTarget);

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
#elif defined(ARCH_ARM)
        int32x4_t vTarget = vdupq_n_s32(targetVal);
        if (outIndicesPtr == 0) {
            int limit = rows - 16;
            for (; i <= limit; i += 16) {
                int32x4_t v0 = vld1q_s32(&data[i]);
                int32x4_t v1 = vld1q_s32(&data[i+4]);
                int32x4_t v2 = vld1q_s32(&data[i+8]);
                int32x4_t v3 = vld1q_s32(&data[i+12]);

                uint32x4_t m0 = vceqq_s32(v0, vTarget);
                uint32x4_t m1 = vceqq_s32(v1, vTarget);
                uint32x4_t m2 = vceqq_s32(v2, vTarget);
                uint32x4_t m3 = vceqq_s32(v3, vTarget);
                
                int32x4_t sum = vaddq_s32((int32x4_t)m0, (int32x4_t)m1);
                sum = vaddq_s32(sum, (int32x4_t)m2);
                sum = vaddq_s32(sum, (int32x4_t)m3);
                
                matchCount -= (vgetq_lane_s32(sum, 0) + vgetq_lane_s32(sum, 1) + 
                               vgetq_lane_s32(sum, 2) + vgetq_lane_s32(sum, 3));
            }
        }
#endif
        // Scalar Tail
        for (; i < rows; i++) {
            if (data[i] == targetVal) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }
        return matchCount;
    }
}