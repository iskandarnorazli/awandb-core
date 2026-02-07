/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include "common.h"
#include <vector>
#include <string>
#include <unordered_map>

class Dictionary {
public:
    // Forward: String -> ID (Fast Ingest)
    std::unordered_map<std::string, int32_t> forward_map;
    
    // Reverse: ID -> String (Fast Lookup/Display)
    std::vector<std::string> reverse_map;

    Dictionary() {}

    // Encode: Get ID for string, creating it if missing
    int32_t encode(const char* str) {
        std::string s(str);
        auto it = forward_map.find(s);
        if (it != forward_map.end()) {
            return it->second;
        }
        
        int32_t id = (int32_t)reverse_map.size();
        reverse_map.push_back(s);
        forward_map[s] = id;
        return id;
    }

    // Decode: Get String for ID
    const char* decode(int32_t id) {
        if (id < 0 || id >= (int32_t)reverse_map.size()) return nullptr;
        return reverse_map[id].c_str();
    }
};

extern "C" {
    // --- LIFECYCLE ---
    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_dictionaryCreateNative(JNIEnv* env, jobject obj) {
        return (jlong)(new (std::nothrow) Dictionary());
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_dictionaryDestroyNative(JNIEnv* env, jobject obj, jlong ptr) {
        if (ptr != 0) delete (Dictionary*)ptr;
    }

    // --- SCALAR OPS ---
    JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_dictionaryEncodeNative(JNIEnv* env, jobject obj, jlong ptr, jstring jStr) {
        Dictionary* dict = (Dictionary*)ptr;
        const char* str = env->GetStringUTFChars(jStr, nullptr);
        int32_t id = dict->encode(str);
        env->ReleaseStringUTFChars(jStr, str);
        return id;
    }

    JNIEXPORT jstring JNICALL Java_org_awandb_core_jni_NativeBridge_dictionaryDecodeNative(JNIEnv* env, jobject obj, jlong ptr, jint id) {
        Dictionary* dict = (Dictionary*)ptr;
        const char* res = dict->decode(id);
        return res ? env->NewStringUTF(res) : nullptr;
    }

    // --- BATCH OPS (The Speedup) ---
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_dictionaryEncodeBatchNative(
        JNIEnv* env, jobject obj, jlong ptr, jobjectArray jStrings, jlong outIdsPtr
    ) {
        Dictionary* dict = (Dictionary*)ptr;
        int32_t* out = (int32_t*)outIdsPtr;
        jsize len = env->GetArrayLength(jStrings);

        for (int i = 0; i < len; i++) {
            jstring js = (jstring)env->GetObjectArrayElement(jStrings, i);
            if (js) {
                const char* cstr = env->GetStringUTFChars(js, nullptr);
                out[i] = dict->encode(cstr);
                env->ReleaseStringUTFChars(js, cstr);
                env->DeleteLocalRef(js);
            } else {
                out[i] = -1; // Null handling
            }
        }
    }
}