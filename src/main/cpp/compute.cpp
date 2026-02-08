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

extern "C" {
    /*
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jint threshold, jlong outIndicesPtr) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        if (colHeaders[colIdx].type != TYPE_INT) return 0;
        int32_t minVal = colHeaders[colIdx].min_int;
        int32_t maxVal = colHeaders[colIdx].max_int;
        int32_t rows = blkHeader->row_count;
        if (maxVal <= threshold) return 0; 
        if (minVal > threshold) {
            if (outIndicesPtr != 0) {
                int* out = (int*)outIndicesPtr;
                for (int i = 0; i < rows; i++) out[i] = i; 
            }
            return rows;
        }
        int* data = (int*)(rawPtr + colHeaders[colIdx].data_offset);
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        size_t i = 0;
        __m256i vThresh = _mm256_set1_epi32(threshold);
        
        if (outIndicesPtr == 0) {
            size_t limit = (size_t)rows - 64; 
            for (; i <= limit; i += 64) {
                __m256i v0 = _mm256_load_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_load_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_load_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_load_si256((__m256i*)&data[i+24]);
                __m256i v4 = _mm256_load_si256((__m256i*)&data[i+32]);
                __m256i v5 = _mm256_load_si256((__m256i*)&data[i+40]);
                __m256i v6 = _mm256_load_si256((__m256i*)&data[i+48]);
                __m256i v7 = _mm256_load_si256((__m256i*)&data[i+56]);
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
            size_t limit = (size_t)rows - 32;
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
                if (mask0) { for(int k=0; k<8; k++) if((mask0>>k)&1) out[matchCount++] = (int)(i+k); }
                if (mask1) { for(int k=0; k<8; k++) if((mask1>>k)&1) out[matchCount++] = (int)(i+8+k); }
                if (mask2) { for(int k=0; k<8; k++) if((mask2>>k)&1) out[matchCount++] = (int)(i+16+k); }
                if (mask3) { for(int k=0; k<8; k++) if((mask3>>k)&1) out[matchCount++] = (int)(i+24+k); }
            }
        }
        for (; i < (size_t)rows; i++) {
            if (data[i] > threshold) {
                if (outIndicesPtr != 0) out[matchCount] = (int)i;
                matchCount++;
            }
        }
        return matchCount;
    }
        */

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanMultiBlockNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jintArray jThresholds, jintArray jCounts) {
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
        size_t i = 0;
        size_t limit = (size_t)rows - 32;
        for (; i < limit; i += 32) {
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
        for (; i < (size_t)rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) counts[q]++;
            }
        }
        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanStringNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jstring search, jlong outIndicesPtr) {
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
        __m128i vTarget = target.toSIMD(); 
        int requiredMask = (sLen <= 12) ? 0xFFFF : 0x00FF;
        int i = 0;
        int limit = rows - 4;
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
                if ((m0 & requiredMask) == requiredMask) { if (data[i].equals(target)) matchCount++; }
                if ((m1 & requiredMask) == requiredMask) { if (data[i+1].equals(target)) matchCount++; }
                if ((m2 & requiredMask) == requiredMask) { if (data[i+2].equals(target)) matchCount++; }
                if ((m3 & requiredMask) == requiredMask) { if (data[i+3].equals(target)) matchCount++; }
            }
        } else {
            for (; i <= limit; i += 4) {
                __m128i v0 = _mm_loadu_si128((__m128i*)&data[i]);
                __m128i v1 = _mm_loadu_si128((__m128i*)&data[i+1]);
                __m128i v2 = _mm_loadu_si128((__m128i*)&data[i+2]);
                __m128i v3 = _mm_loadu_si128((__m128i*)&data[i+3]);
                int m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(v0, vTarget));
                int m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, vTarget));
                int m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(v2, vTarget));
                int m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(v3, vTarget));
                if ((m0 & requiredMask) == requiredMask) { if (data[i].equals(target)) out[matchCount++] = i; }
                if ((m1 & requiredMask) == requiredMask) { if (data[i+1].equals(target)) out[matchCount++] = i+1; }
                if ((m2 & requiredMask) == requiredMask) { if (data[i+2].equals(target)) out[matchCount++] = i+2; }
                if ((m3 & requiredMask) == requiredMask) { if (data[i+3].equals(target)) out[matchCount++] = i+3; }
            }
        }
        for (; i < rows; i++) {
            if (data[i].equals(target)) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }
        env->ReleaseStringUTFChars(search, cstr);
        return matchCount;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_batchRead(JNIEnv* env, jobject obj, jlong colPtr, jlong indicesPtr, jint count, jlong outDataPtr) {
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
        size_t i = 0;
        size_t alignedLimit = (size_t)rows - (rows % 8);
        __m256i thresholdVec = _mm256_set1_epi32(threshold);
        for (; i < alignedLimit; i += 8) {
            __m256i valVec = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i maskVec = _mm256_cmpgt_epi32(valVec, thresholdVec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(maskVec));
            if (mask != 0) matchCount += _mm_popcnt_u32(mask);
        }
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
        size_t alignedLimit = (size_t)rows - (rows % 8);
        for (; i < alignedLimit; i += 8) {
            __m256i vecData = _mm256_loadu_si256((__m256i*)&data[i]);
            for (int q = 0; q < numQueries; q++) {
                __m256i vecThresh = _mm256_set1_epi32(thresholds[q]);
                __m256i vecMask = _mm256_cmpgt_epi32(vecData, vecThresh);
                int mask = _mm256_movemask_ps(_mm256_castsi256_ps(vecMask));
                if (mask != 0) counts[q] += _mm_popcnt_u32(mask);
            }
        }
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

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxHashVectorNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jlong outHashPtr) {
        if (blockPtr == 0 || outHashPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
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
                hash = _mm_crc32_u64(hash, *ptr64++);
                len -= 8;
            }
            if (len > 0) {
                uint8_t* ptr8 = (uint8_t*)ptr64;
                while (len > 0) {
                    hash = _mm_crc32_u8((uint32_t)hash, *ptr8++);
                    len--;
                }
            }
            out[i] = (int64_t)hash;
        }
    }
}