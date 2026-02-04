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

#ifndef BLOCK_H
#define BLOCK_H

#include <cstdint>
#include "german_string.h"

// ------------------------------------------------------------------------
// AWANDB BLOCK FORMAT DEFINITION (HYBRID LAYOUT)
// ------------------------------------------------------------------------

// Magic Number: "AWAN" in ASCII (0x4E415741)
const uint32_t BLOCK_MAGIC = 0x4E415741;
const uint32_t BLOCK_VERSION = 1;

// 1. The Main Block Header (64-byte aligned)
struct BlockHeader {
    uint32_t magic_number;       // "AWAN"
    uint32_t version;            // Version 1
    uint32_t row_count;          // Rows in this block
    uint32_t column_count;       // Columns stored here
    
    // Future Proofing
    uint64_t reserved[6];        
};

// Enum for Column Data Types
enum ColumnType : uint32_t {
    TYPE_INT    = 0,
    TYPE_FLOAT  = 1,
    TYPE_STRING = 2,  // German String (16-byte fixed width)
    TYPE_VECTOR = 3   // [NEW] Fixed-dimension Float Vector (e.g. Embedding)
};

enum CompressionType : uint32_t {
    COMP_NONE    = 0,
    COMP_LZ4     = 1,
    COMP_BITPACK = 2
};

// 2. The Column Header (Describes one column's data)
// Aligned to exactly 64 bytes for Cache Line efficiency
struct ColumnHeader {
    uint32_t col_id;
    uint32_t type;         // ColumnType
    uint32_t compression;
    
    // Stride: Defines step size per row. 
    // Ints = 4, Strings = 16, Vectors = dim * 4.
    uint32_t stride;       

    // Fixed-Width Data Region
    uint64_t data_offset; 
    uint64_t data_length; 
    
    // String Pool (Variable Length Data)
    // Only used if type == TYPE_STRING. Points to the end of the block.
    uint64_t string_pool_offset; 
    uint64_t string_pool_length;
    
    // Zone Map Statistics (Union for type-specific pruning)
    union {
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
    
    // [NEW] Vector Metadata & Padding
    // Previous padding was 8 bytes. We split it here.
    uint32_t vector_dim;   // Dimension for Vectors (e.g., 128, 384, 1536)
    uint32_t reserved;     // Padding to ensure total struct size is 64 bytes.
};

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