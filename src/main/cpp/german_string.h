/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 * * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#ifndef GERMAN_STRING_H
#define GERMAN_STRING_H

#include <cstdint>
#include <cstring>
#include "common.h" // [CRITICAL] Include common to see ARCH definitions

// ==============================================================================
// THE GERMAN STRING (Fixed 16-Byte Layout)
// ==============================================================================
// Layout:
// [0-3]  len      (4 bytes)
// [4-7]  prefix   (4 bytes) - Always populated, even for long strings
// [8-15] payload  (8 bytes) - Union of Pointer (Long) or Suffix (Short)
struct GermanString {
    uint32_t len;
    char prefix[4];
    
    union {
        const char* ptr;    // For Long Strings (> 12 bytes)
        char suffix[8];     // For Short Strings (remaining 8 bytes)
    };

    // Default constructor
    GermanString() : len(0) {
        std::memset(prefix, 0, 4);
        std::memset(suffix, 0, 8);
    }

    /**
      * Constructor: Converts raw C-String to German String logic.
      */
    GermanString(const char* cstr, uint32_t length, const char* poolPtr = nullptr) {
        len = length;
        std::memset(prefix, 0, 4);
        std::memset(suffix, 0, 8); // Important: Clear payload area for clean SIMD

        if (len <= 12) {
            // SHORT: Copy all to prefix + suffix
            if (cstr && len > 0) {
                // 1. Copy first 4 bytes to prefix
                size_t firstChunk = (len < 4) ? len : 4;
                std::memcpy(prefix, cstr, firstChunk);
                
                // 2. Copy remaining bytes to suffix
                if (len > 4) {
                    std::memcpy(suffix, cstr + 4, len - 4);
                }
            }
        } else {
            // LONG: Copy prefix, set ptr
            if (cstr) {
                std::memcpy(prefix, cstr, 4);
            }
            // If Ingest: use poolPtr. If Query: use cstr (transient view).
            ptr = poolPtr ? poolPtr : cstr;
        }
    }
    
    // Fast Equality Check (The "German" Optimization)
    inline bool equals(const GermanString& other) const {
        if (len != other.len) return false;
        
        // 1. FAST PATH: Check Prefix (Bytes 4-8)
        // We cast to uint32_t to compare 4 bytes in one CPU cycle.
        if (*(uint32_t*)prefix != *(uint32_t*)other.prefix) {
            return false;
        }
        
        // 2. SLOW PATH: Full Check
        if (len <= 12) {
            // Check Suffix (Bytes 8-16)
            // We cast to uint64_t to compare the remaining 8 bytes in one cycle.
            return *(uint64_t*)suffix == *(uint64_t*)other.suffix;
        } else {
            // Long String: Follow pointer to Heap
            return std::memcmp(ptr, other.ptr, len) == 0;
        }
    }
    
    // --- SIMD SUPPORT (Cross-Platform) ---
#ifdef ARCH_X86
    // Intel/AMD: Load into 128-bit AVX register
    inline __m128i toSIMD() const {
        return _mm_loadu_si128((const __m128i*)this);
    }
#elif defined(ARCH_ARM)
    // ARM/Apple Silicon: Load into 128-bit NEON register
    inline uint8x16_t toSIMD() const {
        return vld1q_u8((const uint8_t*)this);
    }
#else
    // Fallback stub for generic CPUs (rarely used)
    inline void* toSIMD() const { return nullptr; }
#endif
};

#endif