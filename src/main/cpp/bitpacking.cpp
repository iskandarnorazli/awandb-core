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
#include <immintrin.h>

extern "C" {

    // --- UNPACK 8-BIT -> 32-BIT (AVX2) ---
    // Expands 8 compressed bytes into 8 32-bit integers.
    // Speed: ~30-40 GB/s throughput
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_unpack8To32Native(
        JNIEnv* env, jobject obj, jlong srcPtr, jlong dstPtr, jint count
    ) {
        if (srcPtr == 0 || dstPtr == 0) return;
        
        uint8_t* src = (uint8_t*)srcPtr;
        int32_t* dst = (int32_t*)dstPtr;
        
        int i = 0;
        int limit = count - 32; 
        
        // Process 32 integers (32 bytes) per iteration using AVX2
        for (; i <= limit; i += 32) {
            // Load 32 bytes (256 bits) -> This contains 32 integers!
            __m256i vRaw = _mm256_loadu_si256((__m256i*)&src[i]);
            
            // We need to expand these 8-bit ints into 32-bit ints.
            // AVX2 'pmovzxbd' (Packed Move Zero-Extend Byte to Dword) takes 128-bit chunks.
            
            // Lane 1: Bytes 0-7
            __m128i chunk1 = _mm256_castsi256_si128(vRaw);
            _mm256_storeu_si256((__m256i*)&dst[i], _mm256_cvtepu8_epi32(chunk1));
            
            // Lane 2: Bytes 8-15
            // Extract upper 128 bits isn't direct in C intrinsic for cvt, so we shift/permute or just load simpler.
            // Actually, simpler strategy: Load 8 bytes (64 bits) -> Expand to 256 bits (8 ints)
            
            // RELOAD STRATEGY (Simpler code, still fast)
            // Group 0 (0-7)
            __m128i lo = _mm_loadu_si64((const void*)&src[i]); 
            _mm256_storeu_si256((__m256i*)&dst[i], _mm256_cvtepu8_epi32(lo));
            
            // Group 1 (8-15)
            __m128i lo2 = _mm_loadu_si64((const void*)&src[i+8]); 
            _mm256_storeu_si256((__m256i*)&dst[i+8], _mm256_cvtepu8_epi32(lo2));
            
            // Group 2 (16-23)
            __m128i lo3 = _mm_loadu_si64((const void*)&src[i+16]); 
            _mm256_storeu_si256((__m256i*)&dst[i+16], _mm256_cvtepu8_epi32(lo3));
            
            // Group 3 (24-31)
            __m128i lo4 = _mm_loadu_si64((const void*)&src[i+24]); 
            _mm256_storeu_si256((__m256i*)&dst[i+24], _mm256_cvtepu8_epi32(lo4));
        }
        
        // Scalar Tail
        for (; i < count; i++) {
            dst[i] = (int32_t)src[i];
        }
    }

    // --- UNPACK 16-BIT -> 32-BIT (AVX2) ---
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_unpack16To32Native(
        JNIEnv* env, jobject obj, jlong srcPtr, jlong dstPtr, jint count
    ) {
        if (srcPtr == 0 || dstPtr == 0) return;
        
        uint16_t* src = (uint16_t*)srcPtr;
        int32_t* dst = (int32_t*)dstPtr;
        
        int i = 0;
        int limit = count - 16;
        
        for (; i <= limit; i += 16) {
            // Group 0 (0-7): Load 8 shorts (128 bits) -> Expand to 8 ints (256 bits)
            __m128i v0 = _mm_loadu_si128((__m128i*)&src[i]);
            _mm256_storeu_si256((__m256i*)&dst[i], _mm256_cvtepu16_epi32(v0));
            
            // Group 1 (8-15)
            __m128i v1 = _mm_loadu_si128((__m128i*)&src[i+8]);
            _mm256_storeu_si256((__m256i*)&dst[i+8], _mm256_cvtepu16_epi32(v1));
        }
        
        // Scalar Tail
        for (; i < count; i++) {
            dst[i] = (int32_t)src[i];
        }
    }
}