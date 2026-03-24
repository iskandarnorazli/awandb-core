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

#include <gtest/gtest.h>
#include "buffer_pool.h"

class BufferPoolTest : public ::testing::Test {
protected:
    // Setup a tiny buffer pool for testing: 10 blocks max capacity
    BufferPool pool{10 * BLOCK_SIZE}; 
};

TEST_F(BufferPoolTest, HonorsPinCountDuringEviction) {
    // 1. Fill the pool to exactly 100% capacity
    std::vector<BlockHeader*> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(pool.request_page(i)); // ID = i
    }
    
    EXPECT_EQ(pool.get_usage_percent(), 100);

    // 2. Pin blocks 0 through 8, but UNPIN block 9 (Least Recently Used candidate)
    for (int i = 0; i < 9; ++i) {
        blocks[i]->pin_count++;
    }
    blocks[9]->pin_count = 0; 
    blocks[9]->is_resident = true;

    // 3. Request an 11th block. This should trigger the Hard limit (95%+) and force eviction.
    BlockHeader* new_block = pool.request_page(10);

    // 4. Assertions
    ASSERT_NE(new_block, nullptr) << "Buffer pool failed to evict and allocate new page.";
    
    // Block 9 should be the one evicted because it was the only unpinned block
    EXPECT_FALSE(pool.is_resident(9)); 
    EXPECT_TRUE(pool.is_resident(0)); // Block 0 was pinned, should still be resident
}

TEST_F(BufferPoolTest, TriggersSoftWatermarkBackgroundEviction) {
    // 1. Fill pool to 80% (Passes the 75% Soft Watermark)
    for (int i = 0; i < 8; ++i) {
        pool.request_page(i)->pin_count = 0; // Unpinned, safe to evict
    }

    EXPECT_EQ(pool.get_usage_percent(), 80);

    // 2. Wake the Pacemaker Daemon manually for the test
    pool.trigger_pacemaker_sweep();

    // 3. Assertions
    // The pacemaker should have evicted blocks down to the idle watermark (e.g., 20%)
    EXPECT_LE(pool.get_usage_percent(), 20); 
}