/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

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
        // 1. Calculate Sizes
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

        // 2. Allocate
        uint8_t* rawPtr = (uint8_t*)alloc_aligned(totalBlockSize);
        if (!rawPtr) return 0; // OOM

        // 3. Initialize Headers
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        blkHeader->magic_number = BLOCK_MAGIC;
        blkHeader->version = BLOCK_VERSION;
        blkHeader->row_count = rowCount;
        blkHeader->column_count = colCount;
        std::memset(blkHeader->reserved, 0, sizeof(blkHeader->reserved));

        // 4. Initialize Columns
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

    // 1. Get exact byte size of a Block (for saving to disk)
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

    // 2. Get Row Count from Header
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_getRowCount(JNIEnv* env, jobject obj, jlong blockPtr) {
        if (blockPtr == 0) return 0;
        BlockHeader* header = (BlockHeader*)blockPtr;
        return (jint)header->row_count;
    }

    // 3. Load a Block from Disk
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
    // LEGACY & HELPERS
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

    // ------------------------------------------------------------------------
    // COMPUTE KERNELS (AVX-2 Optimized)
    // ------------------------------------------------------------------------

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanIndicesNative(JNIEnv* env, jobject obj, jlong colPtr, jint rows, jint threshold, jlong outIndicesPtr) {
        if (colPtr == 0) return 0;
        
        int* data = (int*)colPtr;
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;
        int i = 0;
        
        __m256i thresholdVec = _mm256_set1_epi32(threshold);

        for (; i <= rows - 8; i += 8) {
            __m256i valVec = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i maskVec = _mm256_cmpgt_epi32(valVec, thresholdVec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(maskVec));
            
            if (mask != 0) {
                // Manually extract indices (could be optimized with compressstore in AVX-512)
                for (int j = 0; j < 8; j++) {
                    if ((mask >> j) & 1) {
                        out[matchCount++] = i + j;
                    }
                }
            }
        }
        for (; i < rows; i++) {
            if (data[i] > threshold) out[matchCount++] = i;
        }
        return matchCount;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_batchRead(JNIEnv* env, jobject obj, jlong colPtr, jlong indicesPtr, jint count, jlong outDataPtr) {
        if (colPtr == 0 || indicesPtr == 0 || outDataPtr == 0) return;
        
        int* data = (int*)colPtr;
        int* indices = (int*)indicesPtr;
        int* result = (int*)outDataPtr;
        
        // Gather Scatter logic (Scalar fallback for now)
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
    // QUERY FUSION (Shared Scan)
    // ------------------------------------------------------------------------

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanIndicesMulti(
        JNIEnv* env, jobject obj, 
        jlong colPtr, jint rows, 
        jintArray jThresholds, jintArray jCounts
    ) {
        if (colPtr == 0) return;
        
        int* data = (int*)colPtr;
        
        jint* thresholds = (jint*)env->GetPrimitiveArrayCritical(jThresholds, nullptr);
        jint* counts = (jint*)env->GetPrimitiveArrayCritical(jCounts, nullptr);
        jsize numQueries = env->GetArrayLength(jThresholds);

        // Reset counts
        std::memset(counts, 0, numQueries * sizeof(int));

        size_t i = 0;
        size_t alignedLimit = rows - (rows % 8);

        for (; i < alignedLimit; i += 8) {
            __m256i vecData = _mm256_loadu_si256((__m256i*)&data[i]);

            for (int q = 0; q < numQueries; q++) {
                __m256i vecThresh = _mm256_set1_epi32(thresholds[q]);
                __m256i vecMask = _mm256_cmpgt_epi32(vecData, vecThresh);
                int mask = _mm256_movemask_ps(_mm256_castsi256_ps(vecMask));
                
                if (mask != 0) {
                     counts[q] += _mm_popcnt_u32(mask);
                }
            }
        }

        // Handle remaining items
        for (; i < rows; i++) {
            int val = data[i];
            for (int q = 0; q < numQueries; q++) {
                if (val > thresholds[q]) {
                    counts[q]++;
                }
            }
        }

        env->ReleasePrimitiveArrayCritical(jThresholds, thresholds, 0);
        env->ReleasePrimitiveArrayCritical(jCounts, counts, 0);
    }
}