/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

// [CRITICAL WINDOWS FIXES]
// Must be defined BEFORE any includes
#define _CRT_SECURE_NO_WARNINGS  // Fixes "fopen may be unsafe"
#define NOMINMAX                 // Fixes "std::max/min" macro conflicts

#include <jni.h>
#include <immintrin.h> // AVX Intrinsics
#include <nmmintrin.h> // For _mm_crc32_u64
#include <vector>
#include <cstdio>      // For fopen, fwrite, fread
#include <cstring>     // For memcpy
#include <cstdlib>     // For malloc/free
#include <new>         // For std::bad_alloc
#include <limits>      // For std::numeric_limits
#include <thread>

// [CROSS-PLATFORM HEADER GUARD]
#ifdef _WIN32
    #include <windows.h>
    #include <malloc.h>
#else
    #include <unistd.h>
#endif

#include "block.h"     
#include "cuckoo.h"    

// ------------------------------------------------------------------------
// CROSS-PLATFORM ALIGNED MEMORY HELPERS
// ------------------------------------------------------------------------
void* alloc_aligned(size_t size) {
    size_t alignment = 64; 
#ifdef _WIN32
    return _aligned_malloc(size, alignment);
#else
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) return nullptr;
    return ptr;
#endif
}

void free_aligned(void* ptr) {
    if (!ptr) return;
#ifdef _WIN32
    _aligned_free(ptr);
#else
    free(ptr);
#endif
}

extern "C" {

    // ------------------------------------------------------------------------
    // BLOCK MANAGEMENT (Memory Layout)
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_createBlockNative(
        JNIEnv* env, jobject obj, jint rowCount, jint colCount
    ) {
        size_t headerSize = sizeof(BlockHeader);
        size_t colHeadersSize = colCount * sizeof(ColumnHeader);
        size_t metaDataSize = headerSize + colHeadersSize;

        // Align data start to 256-byte boundary
        size_t padding = 0;
        if (metaDataSize % 256 != 0) {
            padding = 256 - (metaDataSize % 256);
        }
        size_t dataStartOffset = metaDataSize + padding;
        
        // [DENSE LAYOUT] 4 bytes per row
        size_t columnSizeBytes = (size_t)rowCount * sizeof(int);
        size_t totalDataSize = (size_t)colCount * columnSizeBytes;
        
        // Total allocation
        size_t totalBlockSize = dataStartOffset + totalDataSize;

        uint8_t* rawPtr = (uint8_t*)alloc_aligned(totalBlockSize);
        if (!rawPtr) return 0; // OOM

        // Zero Data (Crucial for AVX padding)
        std::memset(rawPtr, 0, totalBlockSize);

        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        blkHeader->magic_number = BLOCK_MAGIC;
        blkHeader->version = BLOCK_VERSION;
        blkHeader->row_count = rowCount;
        blkHeader->column_count = colCount;

        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        for (int i = 0; i < colCount; i++) {
            colHeaders[i].col_id = i;
            colHeaders[i].type = TYPE_INT;
            colHeaders[i].compression = COMP_NONE;
            
            // [DENSE] Stride is 4 bytes
            colHeaders[i].data_offset = dataStartOffset + (i * columnSizeBytes);
            colHeaders[i].data_length = columnSizeBytes;
            
            // No String Pool usage in Dense Mode
            colHeaders[i].string_pool_offset = 0;
            colHeaders[i].string_pool_length = 0;
            
            colHeaders[i].min_int = std::numeric_limits<int32_t>::max();
            colHeaders[i].max_int = std::numeric_limits<int32_t>::min();
        }

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
    // STRING INGESTION
    // ------------------------------------------------------------------------
    
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadStringDataNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jobjectArray jStrings
    ) {
        if (blockPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        
        // 1. Get input size
        jsize len = env->GetArrayLength(jStrings);
        
        // 2. Setup Pointers
        ch->type = TYPE_STRING;
        ch->stride = 16; 
        GermanString* fixedData = (GermanString*)(rawPtr + ch->data_offset);
        
        // [CRITICAL FIX] 
        // We assume 'ch->data_length' currently holds the TOTAL allocated capacity (from createBlock).
        // We place the String Pool immediately after the rows we are about to write.
        size_t structBytes = (size_t)len * 16;
        char* poolBase = (char*)fixedData + structBytes;
        
        // Calculate remaining space for the pool
        uint64_t poolLimit = 0;
        if (ch->data_length > structBytes) {
            poolLimit = ch->data_length - structBytes;
        }

        uint64_t currentOffset = 0;

        for (int i = 0; i < len; i++) {
            jstring js = (jstring)env->GetObjectArrayElement(jStrings, i);
            if (!js) {
                fixedData[i] = GermanString(nullptr, 0, nullptr);
                continue;
            }

            const char* cstr = env->GetStringUTFChars(js, nullptr);
            uint32_t sLen = (uint32_t)strlen(cstr);

            if (sLen <= 12) {
                fixedData[i] = GermanString(cstr, sLen, nullptr);
            } else {
                // Check against calculated limit
                if (currentOffset + sLen < poolLimit) {
                    char* poolPtr = poolBase + currentOffset;
                    std::memcpy(poolPtr, cstr, sLen);
                    fixedData[i] = GermanString(cstr, sLen, poolPtr);
                    currentOffset += sLen;
                } else {
                    // Pool Overflow Protection
                    fixedData[i] = GermanString(nullptr, 0, nullptr); 
                }
            }

            env->ReleaseStringUTFChars(js, cstr);
            env->DeleteLocalRef(js);
        }
        
        // [CRITICAL FIX] Update Header Metadata
        ch->string_pool_offset = (uint64_t)((uint8_t*)poolBase - rawPtr);
        ch->string_pool_length = currentOffset;
        
        // Shrink 'data_length' to match the actual valid structs.
        ch->data_length = structBytes;
    }

    // ------------------------------------------------------------------------
    // GERMAN STRING SCANNER (AVX2)
    // ------------------------------------------------------------------------

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
        
        __m128i vTarget = target.toSIMD(); 
        
        int i = 0;
        int limit = rows - 4;
        
        int requiredMask = (sLen <= 12) ? 0xFFFF : 0x00FF;

        if (outIndicesPtr == 0) {
            // --- COUNT ONLY PATH ---
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
            // --- EXTRACTION PATH ---
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

    // ------------------------------------------------------------------------
    // BLOCK I/O & METADATA
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getBlockSize(JNIEnv* env, jobject obj, jlong blockPtr) {
        if (blockPtr == 0) return 0;
        
        BlockHeader* header = (BlockHeader*)blockPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)((uint8_t*)blockPtr + sizeof(BlockHeader));
        
        // Calculate total size based on the last column's data end
        ColumnHeader* lastCol = &colHeaders[header->column_count - 1];
        size_t endOfData = lastCol->data_offset + lastCol->data_length;
        
        return (jlong)endOfData;
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

        void* ptr = alloc_aligned((size_t)fileSize);
        if (!ptr) {
            fclose(file);
            env->ReleaseStringUTFChars(path, filename);
            return 0;
        }

        size_t readBytes = fread(ptr, 1, (size_t)fileSize, file);
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        
        if (readBytes != (size_t)fileSize) {
            free_aligned(ptr);
            return 0;
        }

        return (jlong)ptr;
    }
    
    // ------------------------------------------------------------------------
    // MEMORY HELPERS
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_allocMainStoreNative(JNIEnv* env, jobject obj, jlong num_elements) {
        size_t bytes = (size_t)num_elements * sizeof(int);
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
        memcpy((void*)ptr, scalaData, (size_t)length * sizeof(int));
        env->ReleaseIntArrayElements(jData, scalaData, 0);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_copyToScalaNative(JNIEnv* env, jobject obj, jlong srcPtr, jintArray dstArray, jint len) {
        if (srcPtr == 0) return;
        int* cppData = (int*)srcPtr;
        jint* scalaData = env->GetIntArrayElements(dstArray, nullptr);
        memcpy(scalaData, cppData, (size_t)len * sizeof(int));
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
            if (colHeaders[i].type == TYPE_INT) {
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
        }

        // 2. WRITE TO DISK
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "wb");
        if (!file) { 
            env->ReleaseStringUTFChars(path, filename); 
            return false; 
        }
        
        fwrite((void*)ptr, 1, (size_t)size, file);
        
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        return true;
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_loadColumn(JNIEnv* env, jobject obj, jlong ptr, jlong size, jstring path) {
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "rb");
        if (!file) { env->ReleaseStringUTFChars(path, filename); return false; }
        fread((void*)ptr, 1, (size_t)size, file);
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
    // SMART COMPUTE KERNELS (DENSE INTEGERS)
    // ------------------------------------------------------------------------

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanBlockNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, jint threshold, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0;

        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        
        // Safety: Only scan INTs with this kernel
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

        // Setup Pointers
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

        // Cleanup Tail
        for (; i < (size_t)rows; i++) {
            if (data[i] > threshold) {
                if (outIndicesPtr != 0) out[matchCount] = (int)i;
                matchCount++;
            }
        }
        return matchCount;
    }

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

    // ------------------------------------------------------------------------
    // LEGACY COMPUTE KERNELS (Backward Compatibility)
    // ------------------------------------------------------------------------

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

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanArrayMultiNative(
        JNIEnv* env, jobject obj, 
        jintArray jData, jintArray jThresholds, jintArray jCounts
    ) {
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

    // ------------------------------------------------------------------------
    // CUCKOO FILTER
    // ------------------------------------------------------------------------

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooCreateNative(JNIEnv* env, jobject obj, jint capacity) {
        CuckooFilter* filter = new (std::nothrow) CuckooFilter(capacity);
        return (jlong)filter;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooDestroyNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) delete (CuckooFilter*)ptr;
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
    
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooBuildBatchNative(JNIEnv* env, jobject obj, jlong ptr, jintArray jData) {
        if (ptr == 0) return;
        CuckooFilter* filter = (CuckooFilter*)ptr;
        jint* data = (jint*)env->GetPrimitiveArrayCritical(jData, nullptr);
        jsize len = env->GetArrayLength(jData);
        filter->insert_batch((int32_t*)data, (size_t)len);
        env->ReleasePrimitiveArrayCritical(jData, data, 0);
    }

    // ------------------------------------------------------------------------
    // VECTOR INGESTION
    // ------------------------------------------------------------------------
    
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadVectorDataNative(
        JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jfloatArray jData, jint dim
    ) {
        if (blockPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        
        // Update Header
        ch->type = TYPE_VECTOR;
        ch->vector_dim = (uint32_t)dim;
        ch->stride = dim * sizeof(float);
        
        // Safety check: Pointers
        if (ch->data_offset == 0) return;

        jfloat* srcData = env->GetFloatArrayElements(jData, nullptr);
        jsize count = env->GetArrayLength(jData); // Total floats (rows * dim)
        
        // Copy Data to Block
        float* dstData = (float*)(rawPtr + ch->data_offset);
        std::memcpy(dstData, srcData, (size_t)count * sizeof(float));
        
        // Update metadata
        ch->data_length = (size_t)count * sizeof(float);
        
        env->ReleaseFloatArrayElements(jData, srcData, 0);
    }

    // ------------------------------------------------------------------------
    // VECTOR SEARCH KERNEL (AVX2 FMA)
    // Calculates Dot Product (Cosine Similarity for Normalized Vectors)
    // ------------------------------------------------------------------------
    
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_avxScanVectorCosineNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, jfloatArray jQuery, jfloat threshold, jlong outIndicesPtr
    ) {
        if (blockPtr == 0) return 0;
        
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        
        // Only run on Vector Columns
        if (ch->type != TYPE_VECTOR) return 0;

        int dim = (int)ch->vector_dim;
        int rows = (int)(ch->data_length / ch->stride); 
        
        float* data = (float*)(rawPtr + ch->data_offset);
        int* out = (int*)outIndicesPtr;
        int matchCount = 0;

        jfloat* query = env->GetFloatArrayElements(jQuery, nullptr);
        
        // --- PREPARE ---
        int dimLimit = dim - (dim % 8); 

        for (int i = 0; i < rows; i++) {
            float* vec = data + (i * dim);
            
            // Initialize Accumulators (8 parallel sums)
            __m256 sum = _mm256_setzero_ps();
            
            // Unrolled Dot Product
            for (int d = 0; d < dimLimit; d += 8) {
                __m256 vA = _mm256_loadu_ps(&vec[d]);
                __m256 vB = _mm256_loadu_ps(&query[d]);
                
                // Fused Multiply-Add: sum = (vA * vB) + sum
                sum = _mm256_fmadd_ps(vA, vB, sum);
            }
            
            // Horizontal Sum (Reduce 8 floats to 1 float)
            __m256 shuf = _mm256_permute2f128_ps(sum, sum, 1); // Swap low/high 128
            sum = _mm256_add_ps(sum, shuf);
            sum = _mm256_hadd_ps(sum, sum); // Horizontal add adjacent pairs
            sum = _mm256_hadd_ps(sum, sum); // Final horizontal add
            
            float score = _mm256_cvtss_f32(sum);
            
            // Handle Tails (if dim is not multiple of 8)
            for (int d = dimLimit; d < dim; d++) {
                score += vec[d] * query[d];
            }
            
            // Filter
            if (score >= threshold) {
                if (outIndicesPtr != 0) out[matchCount] = i;
                matchCount++;
            }
        }

        env->ReleaseFloatArrayElements(jQuery, query, 0);
        return matchCount;
    }

    // ------------------------------------------------------------------------
    // HASHING ENGINE (Hardware CRC32)
    // ------------------------------------------------------------------------

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_avxHashVectorNative(
        JNIEnv* env, jobject obj, 
        jlong blockPtr, jint colIdx, jlong outHashPtr
    ) {
        if (blockPtr == 0 || outHashPtr == 0) return;
        
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        
        int stride = ch->stride;
        int rows = (int)(ch->data_length / stride);
        
        uint8_t* data = (uint8_t*)(rawPtr + ch->data_offset);
        int64_t* out = (int64_t*)outHashPtr; // Java Long is 64-bit signed
        
        // --- HASHING LOOP ---
        for (int i = 0; i < rows; i++) {
            uint8_t* item = data + (i * stride);
            uint64_t hash = 0;
            
            int len = stride;
            uint64_t* ptr64 = (uint64_t*)item;
            
            while (len >= 8) {
                // Hardware instruction: Accumulate CRC32
                hash = _mm_crc32_u64(hash, *ptr64++);
                len -= 8;
            }
            
            // Handle remaining bytes
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

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_copyToScalaLongNative(
        JNIEnv* env, jobject obj, jlong srcPtr, jlongArray dstArray, jint len
    ) {
        if (srcPtr == 0) return;
        jlong* scalaData = env->GetLongArrayElements(dstArray, nullptr);
        memcpy(scalaData, (void*)srcPtr, (size_t)len * sizeof(int64_t));
        env->ReleaseLongArrayElements(dstArray, scalaData, 0);
    }

// ------------------------------------------------------------------------
// HARDWARE TOPOLOGY DISCOVERY (Morsel Sizing)
// ------------------------------------------------------------------------

    JNIEXPORT jlongArray JNICALL Java_org_awandb_core_jni_NativeBridge_getSystemTopologyNative(JNIEnv* env, jobject obj) {
        jlong info[2];
        
        // 1. Detect Logical Cores
        unsigned int cores = std::thread::hardware_concurrency();
        info[0] = (cores == 0) ? 4 : cores; // Fallback to 4 if detection fails

        // 2. Detect L3 Cache Size (Cross Platform)
        long l3_size = -1;

#ifdef _WIN32
        // Windows Implementation
        DWORD bufferSize = 0;
        GetLogicalProcessorInformation(nullptr, &bufferSize);
        
        SYSTEM_LOGICAL_PROCESSOR_INFORMATION* buffer = (SYSTEM_LOGICAL_PROCESSOR_INFORMATION*)malloc(bufferSize);
        
        if (GetLogicalProcessorInformation(buffer, &bufferSize)) {
            DWORD count = bufferSize / sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
            for (DWORD i = 0; i < count; i++) {
                if (buffer[i].Relationship == RelationCache && buffer[i].Cache.Level == 3) {
                    l3_size = buffer[i].Cache.Size;
                    break;
                }
            }
        }
        free(buffer);
#else
        // POSIX Implementation
    #ifdef _SC_LEVEL3_CACHE_SIZE
        l3_size = sysconf(_SC_LEVEL3_CACHE_SIZE);
    #endif
#endif

        if (l3_size <= 0) {
            l3_size = 12 * 1024 * 1024; // 12MB Safe Default
        }
        info[1] = l3_size;

        jlongArray result = env->NewLongArray(2);
        env->SetLongArrayRegion(result, 0, 2, info);
        return result;
    }
    
    // ------------------------------------------------------------------------
    // HARDWARE TOPOLOGY
    // ------------------------------------------------------------------------
    
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_initHardwareTopology(JNIEnv* env, jobject obj) {
        // No-op for now
    }
}