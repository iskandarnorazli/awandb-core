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

#include "common.h"
#include "block.h"
#include <limits>
#include <cstring>

extern "C" {

    JNIEXPORT jlong JNICALL Java_org_awandb_core_jni_NativeBridge_compactBlocksNative(
        JNIEnv* env, jobject obj, jlongArray jBlockPtrs, jlongArray jBitmaskPtrs, jint numBlocks
    ) {
        if (numBlocks <= 0 || jBlockPtrs == nullptr || jBitmaskPtrs == nullptr) return 0;

        // [ZERO-COPY] Grab the pointers directly from JVM memory
        jlong* blocks = (jlong*)env->GetPrimitiveArrayCritical(jBlockPtrs, nullptr);
        jlong* bitmasks = (jlong*)env->GetPrimitiveArrayCritical(jBitmaskPtrs, nullptr);

        if (!blocks || !bitmasks) {
            if (blocks) env->ReleasePrimitiveArrayCritical(jBlockPtrs, blocks, 0);
            if (bitmasks) env->ReleasePrimitiveArrayCritical(jBitmaskPtrs, bitmasks, 0);
            return 0;
        }

        // 1. Analyze schema from the first block
        BlockHeader* firstHeader = (BlockHeader*)blocks[0];
        uint32_t colCount = firstHeader->column_count;
        ColumnHeader* firstColHeaders = (ColumnHeader*)((uint8_t*)blocks[0] + sizeof(BlockHeader));

        // 2. Count exact surviving rows across all blocks
        uint32_t totalSurvivingRows = 0;
        for (int b = 0; b < numBlocks; b++) {
            BlockHeader* bh = (BlockHeader*)blocks[b];
            uint8_t* bitmask = (uint8_t*)bitmasks[b];
            
            int rows = bh->row_count;
            if (bitmask == nullptr || bitmasks[b] == 0) {
                totalSurvivingRows += rows;
            } else {
                for (int r = 0; r < rows; r++) {
                    bool isDeleted = (bitmask[r >> 3] & (1 << (r & 7))) != 0;
                    if (!isDeleted) totalSurvivingRows++;
                }
            }
        }

        if (totalSurvivingRows == 0) {
            env->ReleasePrimitiveArrayCritical(jBlockPtrs, blocks, 0);
            env->ReleasePrimitiveArrayCritical(jBitmaskPtrs, bitmasks, 0);
            return 0; // Everything was deleted! Nothing to compact.
        }

        // 3. Allocate the new block based on surviving rows
        size_t headerSize = sizeof(BlockHeader);
        size_t colHeadersSize = colCount * sizeof(ColumnHeader);
        size_t metaDataSize = headerSize + colHeadersSize;
        size_t padding = (metaDataSize % 256 != 0) ? (256 - (metaDataSize % 256)) : 0;
        size_t dataStartOffset = metaDataSize + padding;

        size_t totalDataSize = 0;
        for (uint32_t c = 0; c < colCount; c++) {
            totalDataSize += totalSurvivingRows * firstColHeaders[c].stride;
        }

        size_t totalBlockSize = dataStartOffset + totalDataSize + 512; // +512 for AVX padding
        uint8_t* newRawPtr = (uint8_t*)alloc_aligned(totalBlockSize);
        std::memset(newRawPtr, 0, totalBlockSize);

        BlockHeader* newBh = (BlockHeader*)newRawPtr;
        newBh->magic_number = BLOCK_MAGIC;
        newBh->version = BLOCK_VERSION;
        newBh->row_count = totalSurvivingRows;
        newBh->column_count = colCount;

        ColumnHeader* newCh = (ColumnHeader*)(newRawPtr + sizeof(BlockHeader));
        size_t currentOffset = dataStartOffset;

        // Setup Column Headers
        for (uint32_t c = 0; c < colCount; c++) {
            newCh[c] = firstColHeaders[c]; // Copy metadata (stride, type, etc)
            newCh[c].data_offset = currentOffset;
            newCh[c].data_length = totalSurvivingRows * newCh[c].stride;
            newCh[c].min_int = std::numeric_limits<int32_t>::max();
            newCh[c].max_int = std::numeric_limits<int32_t>::min();
            currentOffset += newCh[c].data_length;
        }

        // 4. Compact the Data (Columnar Extraction)
        for (uint32_t c = 0; c < colCount; c++) {
            if (newCh[c].type == TYPE_INT) {
                int32_t* newColData = (int32_t*)(newRawPtr + newCh[c].data_offset);
                int32_t newIdx = 0;
                int32_t currentMin = std::numeric_limits<int32_t>::max();
                int32_t currentMax = std::numeric_limits<int32_t>::min();

                // Loop through old blocks and copy surviving rows
                for (int b = 0; b < numBlocks; b++) {
                    BlockHeader* bh = (BlockHeader*)blocks[b];
                    ColumnHeader* oldCh = (ColumnHeader*)((uint8_t*)blocks[b] + sizeof(BlockHeader));
                    int32_t* oldColData = (int32_t*)((uint8_t*)blocks[b] + oldCh[c].data_offset);
                    uint8_t* bitmask = (uint8_t*)bitmasks[b];

                    int rows = bh->row_count;
                    for (int r = 0; r < rows; r++) {
                        bool isDeleted = (bitmask != nullptr && bitmasks[b] != 0) && ((bitmask[r >> 3] & (1 << (r & 7))) != 0);
                        if (!isDeleted) {
                            int32_t val = oldColData[r];
                            newColData[newIdx++] = val;
                            
                            // Rebuild Zone Map (Min/Max) on the fly
                            if (val < currentMin) currentMin = val;
                            if (val > currentMax) currentMax = val;
                        }
                    }
                }
                newCh[c].min_int = currentMin;
                newCh[c].max_int = currentMax;
            }
            // TODO: Add TYPE_STRING and TYPE_VECTOR loops when needed.
        }

        // Release JVM Arrays
        env->ReleasePrimitiveArrayCritical(jBlockPtrs, blocks, 0);
        env->ReleasePrimitiveArrayCritical(jBitmaskPtrs, bitmasks, 0);

        return (jlong)newRawPtr;
    }
}