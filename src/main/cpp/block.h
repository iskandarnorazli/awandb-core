/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#ifndef BLOCK_H
#define BLOCK_H

#include <cstdint>

// ------------------------------------------------------------------------
// AWANDB BLOCK FORMAT DEFINITION
// ------------------------------------------------------------------------
// Designed for:
// 1. Direct Mapping (mmap / Unified Memory)
// 2. SIMD Alignment (64-byte boundaries)
// 3. Zone Map Skipping (Min/Max stats)
// ------------------------------------------------------------------------

// Magic Number: "AWAN" in ASCII (0x4E415741)
const uint32_t BLOCK_MAGIC = 0x4E415741;
const uint32_t BLOCK_VERSION = 1;

// 1. The Main Block Header (At the start of every .udb file)
// Aligned to 64 bytes
struct BlockHeader {
    uint32_t magic_number;       // "AWAN"
    uint32_t version;            // Version 1
    uint32_t row_count;          // Number of rows in this block (e.g., 100,000)
    uint32_t column_count;       // Number of columns stored here
    
    // Future Proofing: Reserved space for Cuckoo Filter offset or Checksums
    uint64_t reserved[6];        
    
    // Padding to ensure the struct is exactly 64 bytes
    // (4+4+4+4 + 8*6 = 16 + 48 = 64 bytes)
};

// Enum for Column Data Types
enum ColumnType : uint32_t {
    TYPE_INT    = 0,
    TYPE_FLOAT  = 1,
    TYPE_DICT   = 2  // Dictionary Code (Int representing a String)
};

// Enum for Compression
enum CompressionType : uint32_t {
    COMP_NONE   = 0,
    COMP_LZ4    = 1,
    COMP_BITPACK = 2
};

// 2. The Column Header (Describes one column's data)
// Aligned to 64 bytes
struct ColumnHeader {
    uint32_t col_id;             // Internal ID (0, 1, 2...)
    uint32_t type;               // ColumnType (INT, FLOAT...)
    uint32_t compression;        // CompressionType
    uint32_t padding1;           // Alignment padding

    uint64_t data_offset;        // Byte offset from start of file where data begins
    uint64_t data_length;        // Length of data in bytes
    
    // Zone Map Statistics (Crucial for Skipping Blocks)
    // We use a union so we can store Int or Float stats in the same bytes
    union {
        struct {
            int32_t min_int;
            int32_t max_int;
        };
        struct {
            float min_float;
            float max_float;
        };
    };
    
    // Padding to reach 64 bytes
    // Current usage: 4*4 + 8*2 + 8 = 16 + 16 + 8 = 40 bytes.
    // Need 24 bytes padding.
    uint8_t padding_end[24]; 
};

// ------------------------------------------------------------------------
// MEMORY LAYOUT VISUALIZATION
// ------------------------------------------------------------------------
// [ BlockHeader (64B) ]
// [ ColHeader 0 (64B) ]
// [ ColHeader 1 (64B) ]
// ...
// [ PADDING to 256B Boundary ]
// [ Data Chunk 0 (Aligned) ]
// [ Data Chunk 1 (Aligned) ]
// ------------------------------------------------------------------------

#endif // BLOCK_H