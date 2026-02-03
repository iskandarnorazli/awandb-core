/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include <jni.h>
#include <immintrin.h> // AVX Intrinsics
#include <vector>
#include <cstdio>      // For fopen, fwrite, fread
#include <cstring>     // For memcpy
#include <cstdlib>     // For malloc/free
#include <new>         // For std::bad_alloc
#include <limits>      // For std::numeric_limits
#include "block.h"     // <--- INCLUDE THE NEW HEADER
#include "cuckoo.h"    // <--- INCLUDE CUCKOO HEADER

// ------------------------------------------------------------------------
// CROSS-PLATFORM ALIGNED MEMORY HELPERS
// ------------------------------------------------------------------------
void* alloc_aligned(size_t size) {
    size_t alignment = 64; 
#ifdef _WIN32
    return _aligned_malloc(size, alignment);
#else
    return std::aligned_alloc(alignment, size);
#endif
}

void free_aligned(void* ptr) {
    if (!ptr) return;
#ifdef _WIN32
    _aligned_free(ptr);
#else
    std::free(ptr);
#endif
}

extern "C" {

    // ------------------------------------------------------------------------
    // BLOCK MANAGEMENT (Memory Layout)
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_createBlockNative(JNIEnv* env, jobject obj, jint rowCount, jint colCount) {
        size_t headerSize = sizeof(BlockHeader);
        size_t colHeadersSize = colCount * sizeof(ColumnHeader);
        size_t metaDataSize = headerSize + colHeadersSize;

        // Align data start to 256-byte boundary
        size_t padding = 0;
        if (metaDataSize % 256 != 0) {
            padding = 256 - (metaDataSize % 256);
        }
        size_t dataStartOffset = metaDataSize + padding;
        size_t columnSizeBytes = rowCount * sizeof(int);
        size_t totalDataSize = colCount * columnSizeBytes;
        size_t totalBlockSize = dataStartOffset + totalDataSize;

        uint8_t* rawPtr = (uint8_t*)alloc_aligned(totalBlockSize);
        if (!rawPtr) return 0; // OOM

        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        blkHeader->magic_number = BLOCK_MAGIC;
        blkHeader->version = BLOCK_VERSION;
        blkHeader->row_count = rowCount;
        blkHeader->column_count = colCount;
        std::memset(blkHeader->reserved, 0, sizeof(blkHeader->reserved));

        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        for (int i = 0; i < colCount; i++) {
            colHeaders[i].col_id = i;
            colHeaders[i].type = TYPE_INT;
            colHeaders[i].compression = COMP_NONE;
            colHeaders[i].data_offset = dataStartOffset + (i * columnSizeBytes);
            colHeaders[i].data_length = columnSizeBytes;
            
            // Initialize stats to defaults (will be calculated on save)
            colHeaders[i].min_int = std::numeric_limits<int32_t>::max();
            colHeaders[i].max_int = std::numeric_limits<int32_t>::min();
        }

        // Zero Data
        std::memset(rawPtr + dataStartOffset, 0, totalDataSize);
        return (jlong)rawPtr;
    }

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getColumnPtr(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;

        if (colIdx < 0 || colIdx >= (int)blkHeader->column_count) return 0;

        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        uint64_t offset = colHeaders[colIdx].data_offset;
        return (jlong)(rawPtr + offset);
    }

    // ------------------------------------------------------------------------
    // BLOCK I/O & METADATA
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getBlockSize(JNIEnv* env, jobject obj, jlong blockPtr) {
        if (blockPtr == 0) return 0;
        
        BlockHeader* header = (BlockHeader*)blockPtr;
        
        size_t metaDataSize = sizeof(BlockHeader) + (header->column_count * sizeof(ColumnHeader));
        size_t padding = 0;
        if (metaDataSize % 256 != 0) {
            padding = 256 - (metaDataSize % 256);
        }
        size_t dataStartOffset = metaDataSize + padding;
        size_t totalDataSize = header->column_count * (header->row_count * sizeof(int));
        
        return (jlong)(dataStartOffset + totalDataSize);
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_getRowCount(JNIEnv* env, jobject obj, jlong blockPtr) {
        if (blockPtr == 0) return 0;
        BlockHeader* header = (BlockHeader*)blockPtr;
        return (jint)header->row_count;
    }

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_loadBlockFromFile(JNIEnv* env, jobject obj, jstring path) {
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "rb");
        if (!file) {
            env->ReleaseStringUTFChars(path, filename);
            return 0;
        }

        fseek(file, 0, SEEK_END);
        long fileSize = ftell(file);
        fseek(file, 0, SEEK_SET);

        if (fileSize <= 0) {
             fclose(file);
             env->ReleaseStringUTFChars(path, filename);
             return 0;
        }

        void* ptr = alloc_aligned(fileSize);
        if (!ptr) {
            fclose(file);
            env->ReleaseStringUTFChars(path, filename);
            return 0;
        }

        size_t readBytes = fread(ptr, 1, fileSize, file);
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        
        if (readBytes != fileSize) {
            free_aligned(ptr);
            return 0;
        }

        return (jlong)ptr;
    }
    
    // ------------------------------------------------------------------------
    // MEMORY HELPERS
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_allocMainStoreNative(JNIEnv* env, jobject obj, jlong num_elements) {
        size_t bytes = num_elements * sizeof(int);
        void* ptr = alloc_aligned(bytes);
        if (ptr) std::memset(ptr, 0, bytes);
        return (jlong)ptr;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_freeMainStoreNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) free_aligned((void*)ptr);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadDataNative(JNIEnv* env, jobject obj, jlong ptr, jintArray jData) {
        if (ptr == 0) return;
        jint* scalaData = env->GetIntArrayElements(jData, nullptr);
        jsize length = env->GetArrayLength(jData);
        memcpy((void*)ptr, scalaData, length * sizeof(int));
        env->ReleaseIntArrayElements(jData, scalaData, 0);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_copyToScalaNative(JNIEnv* env, jobject obj, jlong srcPtr, jintArray dstArray, jint len) {
        if (srcPtr == 0) return;
        int* cppData = (int*)srcPtr;
        jint* scalaData = env->GetIntArrayElements(dstArray, nullptr);
        memcpy(scalaData, cppData, len * sizeof(int));
        env->ReleaseIntArrayElements(dstArray, scalaData, 0);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_batchRead(JNIEnv* env, jobject obj, jlong colPtr, jlong indicesPtr, jint count, jlong outDataPtr) {
        if (colPtr == 0 || indicesPtr == 0 || outDataPtr == 0) return;
        int* data = (int*)colPtr;
        int* indices = (int*)indicesPtr;
        int* result = (int*)outDataPtr;
        for (int i = 0; i < count; i++) {
            result[i] = data[indices[i]];
        }
    }

    // ------------------------------------------------------------------------
    // SAVE BLOCK + CALCULATE ZONE MAPS
    // ------------------------------------------------------------------------
    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_saveColumn(JNIEnv* env, jobject obj, jlong ptr, jlong size, jstring path) {
        if (ptr == 0) return false;
        
        // 1. ZONE MAP CALCULATION
        uint8_t* rawPtr = (uint8_t*)ptr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        
        for (uint32_t i = 0; i < blkHeader->column_count; i++) {
            int* data = (int*)(rawPtr + colHeaders[i].data_offset);
            size_t count = blkHeader->row_count; 
            
            int32_t currentMin = std::numeric_limits<int32_t>::max();
            int32_t currentMax = std::numeric_limits<int32_t>::min();
            
            for (size_t r = 0; r < count; r++) {
                int val = data[r];
                if (val < currentMin) currentMin = val;
                if (val > currentMax) currentMax = val;
            }
            
            colHeaders[i].min_int = currentMin;
            colHeaders[i].max_int = currentMax;
        }

        // 2. WRITE TO DISK
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "wb");
        if (!file) { 
            env->ReleaseStringUTFChars(path, filename); 
            return false; 
        }
        
        fwrite((void*)ptr, 1, size, file);
        
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        return true;
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_loadColumn(JNIEnv* env, jobject obj, jlong ptr, jlong size, jstring path) {
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "rb");
        if (!file) { env->ReleaseStringUTFChars(path, filename); return false; }
        fread((void*)ptr, 1, size, file);
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        return true;
    }

    // ------------------------------------------------------------------------
    // METADATA & OPTIMIZATION (Zone Maps)
    // ------------------------------------------------------------------------

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_getZoneMapNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, 
        jintArray outMinMax
    ) {
        if (blockPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        if (colIdx < 0 || colIdx >= (int)blkHeader->column_count) return;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        jint* out = (jint*)env->GetPrimitiveArrayCritical(outMinMax, nullptr);
        out[0] = colHeaders[colIdx].min_int;
        out[1] = colHeaders[colIdx].max_int;
        env->ReleasePrimitiveArrayCritical(outMinMax, out, 0);
    }

    // ------------------------------------------------------------------------
    // SMART COMPUTE KERNELS (With Predicate Pushdown)
    // ------------------------------------------------------------------------

    /**
     * SMART SINGLE SCAN
     * Handles Zone Map checks internally. 
     * If outIndicesPtr is 0, it runs in "Count Only" mode (faster).
     */
    /**
     * SMART SINGLE SCAN (Aggressive Unrolling + Async Prefetch)
     */
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, jint threshold, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0;

        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        
        int32_t minVal = colHeaders[colIdx].min_int;
        int32_t maxVal = colHeaders[colIdx].max_int;
        int32_t rows = blkHeader->row_count;

        // 1. Zone Map Skipping
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
        int i = 0;
        
        __m256i vThresh = _mm256_set1_epi32(threshold);

        // [OPTIMIZATION] Process 32 integers (4 Vectors) per loop
        // 32 ints = 128 Bytes = 2 Cache Lines
        int limit = rows - 32; 

        // A. Count-Only Mode (Fastest)
        if (outIndicesPtr == 0) {
            for (; i <= limit; i += 32) {
                // 1. Async Prefetch: Fetch Next 128 Bytes (2 Cache Lines ahead)
                // _MM_HINT_T0 = Keep in L1 Cache
                _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);
                _mm_prefetch((const char*)&data[i + 48], _MM_HINT_T0);

                // 2. Load 4 Vectors
                __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                // 3. Compare All
                __m256i m0 = _mm256_cmpgt_epi32(v0, vThresh);
                __m256i m1 = _mm256_cmpgt_epi32(v1, vThresh);
                __m256i m2 = _mm256_cmpgt_epi32(v2, vThresh);
                __m256i m3 = _mm256_cmpgt_epi32(v3, vThresh);

                // 4. Popcount Masks
                int mask0 = _mm256_movemask_ps(_mm256_castsi256_ps(m0));
                int mask1 = _mm256_movemask_ps(_mm256_castsi256_ps(m1));
                int mask2 = _mm256_movemask_ps(_mm256_castsi256_ps(m2));
                int mask3 = _mm256_movemask_ps(_mm256_castsi256_ps(m3));

                // 5. Accumulate (Branchless)
                matchCount += _mm_popcnt_u32(mask0);
                matchCount += _mm_popcnt_u32(mask1);
                matchCount += _mm_popcnt_u32(mask2);
                matchCount += _mm_popcnt_u32(mask3);
            }
        } 
        // B. Index Extraction Mode
        else {
            for (; i <= limit; i += 32) {
                _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);
                _mm_prefetch((const char*)&data[i + 48], _MM_HINT_T0);

                __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
                __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
                __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
                __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

                int mask0 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v0, vThresh)));
                int mask1 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v1, vThresh)));
                int mask2 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v2, vThresh)));
                int mask3 = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(v3, vThresh)));

                // Check & Store (Branchy but necessary for extracting indices)
                if (mask0) { for(int k=0; k<8; k++) if((mask0>>k)&1) out[matchCount++] = i+k; }
                if (mask1) { for(int k=0; k<8; k++) if((mask1>>k)&1) out[matchCount++] = i+8+k; }
                if (mask2) { for(int k=0; k<8; k++) if((mask2>>k)&1) out[matchCount++] = i+16+k; }
                if (mask3) { for(int k=0; k<8; k++) if((mask3>>k)&1) out[matchCount++] = i+24+k; }
            }
        }

        // Cleanup Tail
        for (; i < rows; i++) {
            if (data[i] > threshold) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }
        return matchCount;
    }

    /**
     * SMART MULTI SCAN (Fusion + Unrolling + Prefetch)
     */
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanMultiBlockNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, 
        jintArray jThresholds, jintArray jCounts
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

        // Zone Map Check
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
        size_t limit = rows - 32; // Unroll 4x

        for (; i < limit; i += 32) {
            // [ASYNC PREFETCH]
            _mm_prefetch((const char*)&data[i + 32], _MM_HINT_T0);
            _mm_prefetch((const char*)&data[i + 48], _MM_HINT_T0);

            // Load 4 Data Vectors
            __m256i v0 = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i v1 = _mm256_loadu_si256((__m256i*)&data[i+8]);
            __m256i v2 = _mm256_loadu_si256((__m256i*)&data[i+16]);
            __m256i v3 = _mm256_loadu_si256((__m256i*)&data[i+24]);

            // Scan against all Queries
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

        // Tail
        for (; i < rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) counts[q]++;
            }
        }

        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    // ------------------------------------------------------------------------
    // LEGACY COMPUTE KERNELS (Backward Compatibility)
    // ------------------------------------------------------------------------

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanIndicesNative(JNIEnv* env, jobject obj, jlong colPtr, jint rows, jint threshold, jlong outIndicesPtr) {
        if (colPtr == 0) return 0;
        int* data = (int*)colPtr;
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        
        // Simple Scalar Loop is enough for legacy/fallback
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
         
         // Note: Legacy behavior might expect reset, but let's stick to safe accumulation logic or just simple impl
         std::memset(counts, 0, numQueries * sizeof(int)); // Legacy usually implies single-block logic
         
         for (int i = 0; i < rows; i++) {
             int val = data[i];
             for (int q = 0; q < numQueries; q++) {
                 if (val > thresholds[q]) counts[q]++;
             }
         }
         env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
         env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    // ------------------------------------------------------------------------
    // RAM SCAN (SINGLE QUERY)
    // ------------------------------------------------------------------------
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanArrayNative(
        JNIEnv* env, jobject obj, 
        jintArray jData, 
        jint threshold
    ) {
        if (jData == NULL) return 0;

        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jsize rows = env->GetArrayLength(jData);
        
        int matchCount = 0;
        size_t i = 0;
        size_t alignedLimit = rows - (rows % 8);
        __m256i thresholdVec = _mm256_set1_epi32(threshold);

        // AVX2 Loop
        for (; i < alignedLimit; i += 8) {
            __m256i valVec = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i maskVec = _mm256_cmpgt_epi32(valVec, thresholdVec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(maskVec));
            if (mask != 0) matchCount += _mm_popcnt_u32(mask);
        }

        // Scalar Fallback
        for (; i < rows; i++) {
            if (data[i] > threshold) matchCount++;
        }

        env->ReleasePrimitiveArrayCritical(jData, data, 0);
        return matchCount;
    }

    // ------------------------------------------------------------------------
    // RAM SCAN KERNEL (For Delta Buffer)
    // ------------------------------------------------------------------------
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanArrayMultiNative(
        JNIEnv* env, jobject obj, 
        jintArray jData,          // <--- Raw RAM Array
        jintArray jThresholds, 
        jintArray jCounts
    ) {
        if (jData == NULL) return;

        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jint* thresholds = (jint*)env->GetPrimitiveArrayCritical(jThresholds, nullptr);
        jint* counts = (jint*)env->GetPrimitiveArrayCritical(jCounts, nullptr);
        
        jsize rows = env->GetArrayLength(jData);
        jsize numQueries = env->GetArrayLength(jThresholds);

        // We do NOT reset counts (we accumulate)
        // We do NOT do Zone Map checks (RAM is unsorted/unindexed)

        size_t i = 0;
        size_t alignedLimit = rows - (rows % 8);

        // AVX2 Loop
        for (; i < alignedLimit; i += 8) {
            __m256i vecData = _mm256_loadu_si256((__m256i*)&data[i]);

            for (int q = 0; q < numQueries; q++) {
                __m256i vecThresh = _mm256_set1_epi32(thresholds[q]);
                __m256i vecMask = _mm256_cmpgt_epi32(vecData, vecThresh);
                int mask = _mm256_movemask_ps(_mm256_castsi256_ps(vecMask));
                
                if (mask != 0) counts[q] += _mm_popcnt_u32(mask);
            }
        }

        // Scalar Fallback
        for (; i < rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) counts[q]++;
            }
        }

        env->ReleasePrimitiveArrayCritical(jData, data, 0);
        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }

    // ------------------------------------------------------------------------
    // CUCKOO FILTER (O(1) Point Index)
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooCreateNative(JNIEnv* env, jobject obj, jint capacity) {
        CuckooFilter* filter = new (std::nothrow) CuckooFilter(capacity);
        return (jlong)filter;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooDestroyNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) {
            delete (CuckooFilter*)ptr;
        }
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooInsertNative(JNIEnv* env, jobject obj, jlong ptr, jint key) {
        if (ptr == 0) return false;
        return ((CuckooFilter*)ptr)->insert(key);
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooContainsNative(JNIEnv* env, jobject obj, jlong ptr, jint key) {
        if (ptr == 0) return false;
        return ((CuckooFilter*)ptr)->contains(key);
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooSaveNative(JNIEnv* env, jobject obj, jlong ptr, jstring path) {
        if (ptr == 0) return false;
        const char* p = env->GetStringUTFChars(path, nullptr);
        bool res = ((CuckooFilter*)ptr)->save(p);
        env->ReleaseStringUTFChars(path, p);
        return res;
    }

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooLoadNative(JNIEnv* env, jobject obj, jstring path) {
        const char* p = env->GetStringUTFChars(path, nullptr);
        CuckooFilter* filter = CuckooFilter::load(p);
        env->ReleaseStringUTFChars(path, p);
        return (jlong)filter;
    }
    
    /**
     * Batch Build: Uses Software Prefetching for speed
     */
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooBuildBatchNative(JNIEnv* env, jobject obj, jlong ptr, jintArray jData) {
        if (ptr == 0) return;
        CuckooFilter* filter = (CuckooFilter*)ptr;
        
        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jsize len = env->GetArrayLength(jData);
        
        // [OPTIMIZATION] Use the new prefetching batcher
        filter->insert_batch((int32_t*)data, (size_t)len);
        
        env->ReleasePrimitiveArrayCritical(jData, data, 0);
    }
}