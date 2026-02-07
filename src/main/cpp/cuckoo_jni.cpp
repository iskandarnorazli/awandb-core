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
#include "cuckoo.h"

extern "C" {
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

    // [CRITICAL FIX] 
    // Java expects int[] (4 bytes per element), but C++ was writing bytes (1 byte).
    // We must cast the output pointer to int32_t* and write 32-bit integers.
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_cuckooProbeBatchNative(
        JNIEnv* env, jobject obj, jlong ptr, jlong keysPtr, jint count, jlong outPtr
    ) {
        if (ptr == 0 || keysPtr == 0 || outPtr == 0) return;
        
        CuckooFilter* filter = (CuckooFilter*)ptr;
        int32_t* keys = (int32_t*)keysPtr;
        int32_t* out = (int32_t*)outPtr; // Treat output as INT array (4 bytes stride)
        
        // We cannot use the default 'contains_batch' if it writes bytes.
        // We run the loop here to ensure 32-bit writes.
        // (Compiler will vectorize this loop automatically via AVX2)
        for(int i=0; i<count; i++) {
            out[i] = filter->contains(keys[i]) ? 1 : 0;
        }
    }
}