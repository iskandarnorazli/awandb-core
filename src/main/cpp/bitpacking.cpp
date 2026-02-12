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

// Note: <immintrin.h> and <arm_neon.h> are handled in common.h

extern "C" {

    // --- UNPACK 8-BIT -> 32-BIT (Hybrid) ---
    // Expands 8 compressed bytes into 8 32-bit integers.
    // Speed: ~30-40 GB/s throughput
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_unpack8To32Native(
        JNIEnv* env, jobject obj, jlong srcPtr, jlong dstPtr, jint count
    ) {
        if (srcPtr == 0 || dstPtr == 0) return;
        
        uint8_t* src = (uint8_t*)srcPtr;
        int32_t* dst = (int32_t*)dstPtr;
        
        int i = 0;

// =========================================================
// PATH A: INTEL / AMD (AVX2 - 256 bit)
// =========================================================
#ifdef ARCH_X86
        int limit = count - 32; 
        
        // Process 32 integers (32 bytes) per iteration using AVX2
        for (; i <= limit; i += 32) {
            // RELOAD STRATEGY (Simpler code, still fast)
            // Load 8 bytes (64 bits) -> Expand to 256 bits (8 ints)
            
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

// =========================================================
// PATH B: APPLE SILICON / ARM (NEON - 128 bit)
// =========================================================
#elif defined(ARCH_ARM)
        int limit = count - 16;
        for (; i <= limit; i += 16) {
            // Load 16 bytes (128 bits) -> contains 16 integers
            uint8x16_t raw = vld1q_u8(&src[i]);

            // Step 1: Widen 8 -> 16 bit
            // vmovl_u8 takes lower/upper half (8x8) and widens to 8x16
            uint16x8_t mid_low  = vmovl_u8(vget_low_u8(raw));
            uint16x8_t mid_high = vmovl_u8(vget_high_u8(raw));

            // Step 2: Widen 16 -> 32 bit
            // Expand the mid_low part (first 8 ints)
            uint32x4_t dst0 = vmovl_u16(vget_low_u16(mid_low));   // 0-3
            uint32x4_t dst1 = vmovl_u16(vget_high_u16(mid_low));  // 4-7

            // Expand the mid_high part (last 8 ints)
            uint32x4_t dst2 = vmovl_u16(vget_low_u16(mid_high));  // 8-11
            uint32x4_t dst3 = vmovl_u16(vget_high_u16(mid_high)); // 12-15

            // Step 3: Store
            vst1q_u32((uint32_t*)&dst[i],    dst0);
            vst1q_u32((uint32_t*)&dst[i+4],  dst1);
            vst1q_u32((uint32_t*)&dst[i+8],  dst2);
            vst1q_u32((uint32_t*)&dst[i+12], dst3);
        }
#endif
        
        // Scalar Tail
        for (; i < count; i++) {
            dst[i] = (int32_t)src[i];
        }
    }

    // --- UNPACK 16-BIT -> 32-BIT (Hybrid) ---
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_unpack16To32Native(
        JNIEnv* env, jobject obj, jlong srcPtr, jlong dstPtr, jint count
    ) {
        if (srcPtr == 0 || dstPtr == 0) return;
        
        uint16_t* src = (uint16_t*)srcPtr;
        int32_t* dst = (int32_t*)dstPtr;
        
        int i = 0;

// =========================================================
// PATH A: INTEL / AMD (AVX2)
// =========================================================
#ifdef ARCH_X86
        int limit = count - 16;
        for (; i <= limit; i += 16) {
            // Group 0 (0-7): Load 8 shorts (128 bits) -> Expand to 8 ints (256 bits)
            __m128i v0 = _mm_loadu_si128((__m128i*)&src[i]);
            _mm256_storeu_si256((__m256i*)&dst[i], _mm256_cvtepu16_epi32(v0));
            
            // Group 1 (8-15)
            __m128i v1 = _mm_loadu_si128((__m128i*)&src[i+8]);
            _mm256_storeu_si256((__m256i*)&dst[i+8], _mm256_cvtepu16_epi32(v1));
        }

// =========================================================
// PATH B: APPLE SILICON / ARM (NEON)
// =========================================================
#elif defined(ARCH_ARM)
        int limit = count - 8;
        for (; i <= limit; i += 8) {
            // Load 8 shorts (128 bits)
            uint16x8_t raw = vld1q_u16(&src[i]);

            // Widen 16 -> 32 bit
            uint32x4_t dst0 = vmovl_u16(vget_low_u16(raw));  // 0-3
            uint32x4_t dst1 = vmovl_u16(vget_high_u16(raw)); // 4-7

            // Store
            vst1q_u32((uint32_t*)&dst[i],   dst0);
            vst1q_u32((uint32_t*)&dst[i+4], dst1);
        }
#endif
        
        // Scalar Tail
        for (; i < count; i++) {
            dst[i] = (int32_t)src[i];
        }
    }
}