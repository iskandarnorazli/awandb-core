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

#pragma once

// [CRITICAL WINDOWS FIXES]
#define _CRT_SECURE_NO_WARNINGS
#define NOMINMAX

#include <jni.h>
#include <vector>
#include <cstdio>      
#include <cstring>     
#include <cstdlib>     
#include <new>         
#include <limits>      
#include <thread>
#include <cmath>       

// --- 1. ARCHITECTURE DETECTION ---
// We detect if we are compiling for Intel/AMD (AVX2) or ARM (NEON)
#if defined(__x86_64__) || defined(_M_X64)
    #define ARCH_X86 1
    #include <immintrin.h> // AVX, AVX2, SSE
    #include <nmmintrin.h> // POPCNT
#elif defined(__aarch64__) || defined(_M_ARM64)
    #define ARCH_ARM 1
    #include <arm_neon.h>  // NEON Intrinsics
#endif

// --- 2. NEON COMPATIBILITY HELPERS ---
// ARM NEON does not have a direct equivalent to _mm_movemask_epi8.
// We must emulate it to support the same bit-packing logic used in bitpacking.cpp.
#ifdef ARCH_ARM
inline int neon_movemask_u8(uint8x16_t input) {
    // Create a vector with powers of 2: [1, 2, 4, 8, ...]
    const uint8_t __attribute__((aligned(16))) powers[16] = 
        { 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128 };
    uint8x16_t powVec = vld1q_u8(powers);

    // Mask the input (keep only high bits if they were set)
    uint8x16_t masked = vandq_u8(input, powVec);

    // Pairwise add until we get scalar
    uint8x16_t pairs = vpaddq_u8(masked, masked);
    uint8x16_t quads = vpaddq_u8(pairs, pairs);
    uint8x16_t octs  = vpaddq_u8(quads, quads);
    
    // Extract lower and upper 8 bytes
    uint8_t low = vgetq_lane_u8(octs, 0);
    uint8_t high = vgetq_lane_u8(octs, 8);
    
    return low | (high << 8);
}
#endif

#ifdef _WIN32
    #include <windows.h>
    #include <malloc.h>
#else
    #include <unistd.h>
#endif

#include "block.h"     
#include "cuckoo.h"    

// Shared Memory Allocators
void* alloc_aligned(size_t size);
void free_aligned(void* ptr);