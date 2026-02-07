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
}