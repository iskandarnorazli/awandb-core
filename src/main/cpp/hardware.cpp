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

// [NEW] macOS headers for sysctl
#if defined(__APPLE__)
    #include <sys/types.h>
    #include <sys/sysctl.h>
#endif

extern "C" {
    JNIEXPORT jlongArray JNICALL Java_org_awandb_core_jni_NativeBridge_getSystemTopologyNative(JNIEnv* env, jobject obj) {
        jlong info[2];
        unsigned int cores = std::thread::hardware_concurrency();
        info[0] = (cores == 0) ? 4 : cores; 
        long l3_size = -1;

#ifdef _WIN32
        // --- Windows Logic (Unchanged) ---
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

#elif defined(__APPLE__)
        // --- [NEW] macOS Logic (Intel & Apple Silicon) ---
        int64_t cacheSize = 0;
        size_t size = sizeof(cacheSize);
        
        // 1. Try L3 first (Intel Macs)
        if (sysctlbyname("hw.l3cachesize", &cacheSize, &size, NULL, 0) == 0 && cacheSize > 0) {
            l3_size = cacheSize;
        } 
        // 2. Fallback to L2 (Apple Silicon M1/M2/M3)
        // M-series chips don't strictly have "L3" in the generic sense; 
        // they have huge L2 (12MB+) and SLC. Optimizing for L2 size is safer here.
        else if (sysctlbyname("hw.l2cachesize", &cacheSize, &size, NULL, 0) == 0 && cacheSize > 0) {
             l3_size = cacheSize;
        }

#else
        // --- Linux Logic ---
        #ifdef _SC_LEVEL3_CACHE_SIZE
            l3_size = sysconf(_SC_LEVEL3_CACHE_SIZE);
        #endif
#endif

        // Default Fallback (12MB)
        if (l3_size <= 0) { l3_size = 12 * 1024 * 1024; }
        
        info[1] = l3_size;
        jlongArray result = env->NewLongArray(2);
        env->SetLongArrayRegion(result, 0, 2, info);
        return result;
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_initHardwareTopology(JNIEnv* env, jobject obj) {
        // No-op for now
    }
}