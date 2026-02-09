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
#include <cstdio>
#include <limits>
#include <cstring> // for memcpy, memset

extern "C" {

    // --- Block Management ---
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_createBlockNative(
        JNIEnv* env, jobject obj, jint rowCount, jint colCount
    ) {
        size_t headerSize = sizeof(BlockHeader);
        size_t colHeadersSize = colCount * sizeof(ColumnHeader);
        size_t metaDataSize = headerSize + colHeadersSize;
        
        size_t padding = (metaDataSize % 256 != 0) ? (256 - (metaDataSize % 256)) : 0;
        size_t dataStartOffset = metaDataSize + padding;
        
        size_t columnSizeBytes = (size_t)rowCount * sizeof(int);
        size_t totalDataSize = (size_t)colCount * columnSizeBytes;
        size_t totalBlockSize = dataStartOffset + totalDataSize;

        uint8_t* rawPtr = (uint8_t*)alloc_aligned(totalBlockSize);
        if (!rawPtr) return 0; 
        std::memset(rawPtr, 0, totalBlockSize);

        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        blkHeader->magic_number = BLOCK_MAGIC;
        blkHeader->version = BLOCK_VERSION;
        blkHeader->row_count = rowCount;
        blkHeader->column_count = colCount;

        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        size_t currentOffset = dataStartOffset;

        for (int i = 0; i < colCount; i++) {
            colHeaders[i].col_id = i;
            colHeaders[i].type = TYPE_INT; 
            colHeaders[i].compression = COMP_NONE;
            colHeaders[i].data_offset = currentOffset;
            colHeaders[i].data_length = columnSizeBytes;
            
            // [CRITICAL FIX] Set Stride to 4. 
            // If this is 0, TableScan reads the first rows forever.
            colHeaders[i].stride = 4; 
            
            colHeaders[i].string_pool_offset = 0;
            colHeaders[i].string_pool_length = 0;
            colHeaders[i].min_int = std::numeric_limits<int32_t>::max();
            colHeaders[i].max_int = std::numeric_limits<int32_t>::min();

            currentOffset += columnSizeBytes;
        }

        return (jlong)rawPtr;
    }

    // --- Data Loading ---
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadDataNative(JNIEnv* env, jobject obj, jlong ptr, jintArray jData) {
        if (ptr == 0) return;
        jint* scalaData = env->GetIntArrayElements(jData, nullptr);
        jsize length = env->GetArrayLength(jData);
        std::memcpy((void*)ptr, scalaData, (size_t)length * sizeof(int));
        env->ReleaseIntArrayElements(jData, scalaData, 0);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_copyToScalaNative(JNIEnv* env, jobject obj, jlong srcPtr, jintArray dstArray, jint len) {
        if (srcPtr == 0) return;
        int* cppData = (int*)srcPtr;
        jint* scalaData = env->GetIntArrayElements(dstArray, nullptr);
        std::memcpy(scalaData, cppData, (size_t)len * sizeof(int));
        env->ReleaseIntArrayElements(dstArray, scalaData, 0);
    }

    // [RESTORED] This function was missing in the previous partial snippet
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_copyToScalaLongNative(JNIEnv* env, jobject obj, jlong srcPtr, jlongArray dstArray, jint len) {
        if (srcPtr == 0) return;
        jlong* scalaData = env->GetLongArrayElements(dstArray, nullptr);
        std::memcpy(scalaData, (void*)srcPtr, (size_t)len * sizeof(int64_t));
        env->ReleaseLongArrayElements(dstArray, scalaData, 0);
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadStringDataNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jobjectArray jStrings) {
        if (blockPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        jsize len = env->GetArrayLength(jStrings);
        ch->type = TYPE_STRING;
        ch->stride = 16; 
        GermanString* fixedData = (GermanString*)(rawPtr + ch->data_offset);
        size_t structBytes = (size_t)len * 16;
        char* poolBase = (char*)fixedData + structBytes;
        uint64_t poolLimit = (ch->data_length > structBytes) ? (ch->data_length - structBytes) : 0;
        uint64_t currentOffset = 0;

        for (int i = 0; i < len; i++) {
            jstring js = (jstring)env->GetObjectArrayElement(jStrings, i);
            if (!js) { fixedData[i] = GermanString(nullptr, 0, nullptr); continue; }
            const char* cstr = env->GetStringUTFChars(js, nullptr);
            uint32_t sLen = (uint32_t)strlen(cstr);
            if (sLen <= 12) {
                fixedData[i] = GermanString(cstr, sLen, nullptr);
            } else {
                if (currentOffset + sLen < poolLimit) {
                    char* poolPtr = poolBase + currentOffset;
                    std::memcpy(poolPtr, cstr, sLen);
                    fixedData[i] = GermanString(cstr, sLen, poolPtr);
                    currentOffset += sLen;
                } else { fixedData[i] = GermanString(nullptr, 0, nullptr); }
            }
            env->ReleaseStringUTFChars(js, cstr);
            env->DeleteLocalRef(js);
        }
        ch->string_pool_offset = (uint64_t)((uint8_t*)poolBase - rawPtr);
        ch->string_pool_length = currentOffset;
        ch->data_length = structBytes;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_loadVectorDataNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jfloatArray jData, jint dim) {
        if (blockPtr == 0) return;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        ColumnHeader* ch = &((ColumnHeader*)(rawPtr + sizeof(BlockHeader)))[colIdx];
        ch->type = TYPE_VECTOR;
        ch->vector_dim = (uint32_t)dim;
        ch->stride = dim * sizeof(float);
        if (ch->data_offset == 0) return;
        jfloat* srcData = env->GetFloatArrayElements(jData, nullptr);
        jsize count = env->GetArrayLength(jData); 
        float* dstData = (float*)(rawPtr + ch->data_offset);
        std::memcpy(dstData, srcData, (size_t)count * sizeof(float));
        ch->data_length = (size_t)count * sizeof(float);
        env->ReleaseFloatArrayElements(jData, srcData, 0);
    }

    // --- IO & Metadata ---
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getColumnPtr(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx) {
        if (blockPtr == 0) return 0;
        uint8_t* rawPtr = (uint8_t*)blockPtr;
        BlockHeader* blkHeader = (BlockHeader*)rawPtr;
        if (colIdx < 0 || colIdx >= (int)blkHeader->column_count) return 0;
        ColumnHeader* colHeaders = (ColumnHeader*)(rawPtr + sizeof(BlockHeader));
        uint64_t offset = colHeaders[colIdx].data_offset;
        return (jlong)(rawPtr + offset);
    }

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_getBlockSize(JNIEnv* env, jobject obj, jlong blockPtr) {
        if (blockPtr == 0) return 0;
        BlockHeader* header = (BlockHeader*)blockPtr;
        size_t metaSize = sizeof(BlockHeader) + (header->column_count * sizeof(ColumnHeader));
        size_t padding = (metaSize % 256 != 0) ? (256 - (metaSize % 256)) : 0;
        ColumnHeader* colHeaders = (ColumnHeader*)((uint8_t*)blockPtr + sizeof(BlockHeader));
        ColumnHeader* lastCol = &colHeaders[header->column_count - 1];
        return (jlong)(lastCol->data_offset + lastCol->data_length);
    }

    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_getRowCount(JNIEnv* env, jobject obj, jlong blockPtr) {
        return (blockPtr == 0) ? 0 : (jint)((BlockHeader*)blockPtr)->row_count;
    }

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_loadBlockFromFile(JNIEnv* env, jobject obj, jstring path) {
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "rb");
        if (!file) { env->ReleaseStringUTFChars(path, filename); return 0; }
        fseek(file, 0, SEEK_END);
        long fileSize = ftell(file);
        fseek(file, 0, SEEK_SET);
        if (fileSize <= 0) { fclose(file); env->ReleaseStringUTFChars(path, filename); return 0; }
        void* ptr = alloc_aligned((size_t)fileSize);
        if (!ptr) { fclose(file); env->ReleaseStringUTFChars(path, filename); return 0; }
        size_t readBytes = fread(ptr, 1, (size_t)fileSize, file);
        fclose(file);
        env->ReleaseStringUTFChars(path, filename);
        return (readBytes == (size_t)fileSize) ? (jlong)ptr : 0;
    }

    JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_saveColumn(JNIEnv* env, jobject obj, jlong ptr, jlong size, jstring path) {
        if (ptr == 0) return false;
        // Calc Min/Max
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
        const char* filename = env->GetStringUTFChars(path, nullptr);
        FILE* file = fopen(filename, "wb");
        if (!file) { env->ReleaseStringUTFChars(path, filename); return false; }
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

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_getZoneMapNative(JNIEnv* env, jobject obj, jlong blockPtr, jint colIdx, jintArray outMinMax) {
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
}