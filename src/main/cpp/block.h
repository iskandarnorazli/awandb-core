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

#ifndef BLOCK_H
#define BLOCK_H

#include <cstdint>
#include <atomic>
#include "german_string.h"

// ------------------------------------------------------------------------
// AWANDB BLOCK FORMAT DEFINITION (HYBRID LAYOUT)
// ------------------------------------------------------------------------

// Magic Number: "AWAN" in ASCII (0x4E415741)
const uint32_t BLOCK_MAGIC = 0x4E415741;
const uint32_t BLOCK_VERSION = 1;

// 1. The Main Block Header
// Explicitly aligned to exactly 64 bytes for optimal L1 Cache fetching
struct alignas(64) BlockHeader {
    uint32_t magic_number;       // 4B | Offset: 0
    uint32_t version;            // 4B | Offset: 4
    uint32_t row_count;          // 4B | Offset: 8
    uint32_t column_count;       // 4B | Offset: 12
    
    // --- Phase 6 Buffer Pool Metadata ---
    int32_t page_id;             // 4B | Offset: 16
    std::atomic<int32_t> pin_count{0}; // 4B | Offset: 20
    uint64_t memory_footprint_bytes{4096}; // 8B | Offset: 24 (uint64_t ensures 64-bit size across platforms)
    
    bool is_resident{false};     // 1B | Offset: 32
    uint8_t padding1[7];         // 7B | Offset: 33 (Explicit padding to align the next block to 8-bytes)
    
    // Future Proofing / Padding to exact 64 Bytes total
    uint64_t reserved[3];        // 24B| Offset: 40 
};
// Total Size: 16 + 16 + 8 + 24 = 64 Bytes

// Enum for Column Data Types
enum ColumnType : uint32_t {
    TYPE_INT    = 0,
    TYPE_FLOAT  = 1,
    TYPE_STRING = 2,  // German String (16-byte fixed width)
    TYPE_VECTOR = 3   // Fixed-dimension Float Vector (e.g. Embedding)
};

enum CompressionType : uint32_t {
    COMP_NONE    = 0,
    COMP_LZ4     = 1,
    COMP_BITPACK = 2
};

// 2. The Column Header (Describes one column's data)
// Explicitly aligned to exactly 64 bytes for Cache Line efficiency
struct alignas(64) ColumnHeader {
    uint32_t col_id;             // 4B | Offset: 0
    uint32_t type;               // 4B | Offset: 4 (ColumnType)
    uint32_t compression;        // 4B | Offset: 8
    
    // Stride: Defines step size per row. 
    // Ints = 4, Strings = 16, Vectors = dim * 4.
    uint32_t stride;             // 4B | Offset: 12

    // Fixed-Width Data Region
    uint64_t data_offset;        // 8B | Offset: 16
    uint64_t data_length;        // 8B | Offset: 24
    
    // String Pool (Variable Length Data)
    // Only used if type == TYPE_STRING. Points to the end of the block.
    uint64_t string_pool_offset; // 8B | Offset: 32
    uint64_t string_pool_length; // 8B | Offset: 40
    
    // Zone Map Statistics (Union for type-specific pruning)
    union {                      // 8B | Offset: 48
        // Integer Stats
        struct {
            int32_t min_int;
            int32_t max_int;
        };
        // Float Stats
        struct {
            float min_float;
            float max_float;
        };
        // String Stats (First 4 chars only)
        struct {
            char min_prefix[4];
            char max_prefix[4];
        };
    };
    
    // Vector Metadata & Padding
    uint32_t vector_dim;   // 4B | Offset: 56 | Dimension for Vectors (e.g., 128, 384, 1536)
    uint32_t reserved;     // 4B | Offset: 60 | Padding to ensure total struct size is 64 bytes.
};
// Total Size: 16 + 16 + 16 + 8 + 8 = 64 Bytes

// ------------------------------------------------------------------------
// MEMORY LAYOUT VISUALIZATION
// ------------------------------------------------------------------------
// [ BlockHeader (64B) ]
// [ ColHeader 0 (64B) ] ... [ ColHeader N (64B) ]
// [ PADDING to 256B Boundary ]
// [ Col 0 Fixed Data (Ints: 4B / Strings: 16B / Vec128: 512B) ]
// [ Col 1 Fixed Data ... ]
// [ ... ]
// [ String Pool (Variable Length Blob) ]
// ------------------------------------------------------------------------

#endif // BLOCK_H