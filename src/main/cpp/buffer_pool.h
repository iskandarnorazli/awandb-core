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

#include <atomic>
#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>

#include "block.h"

class BufferPool {
private:
    size_t max_bytes{0};
    size_t current_bytes{0};

    std::unordered_map<int, BlockHeader*> page_table;
    
    // For the real engine: map memory pointers to page IDs
    std::unordered_map<BlockHeader*, int> pointer_to_page;
    int next_page_id = 10000; // IDs for native blocks 

    // MRU (Most Recently Used) is pushed to the back()
    // LRU (Least Recently Used) stays at the begin()
    std::list<int> lru_list; 
    std::mutex pool_mutex;

    // --- SINGLETON SETUP ---
    BufferPool() = default;
    ~BufferPool() = default;
    
    // Disable copy/move
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    void evict_if_needed(size_t target_usage_percent) {
        if (max_bytes == 0) return;
        size_t target_bytes = (max_bytes * target_usage_percent) / 100;
        
        // Sweep forward starting from the LRU (begin)
        auto it = lru_list.begin();
        while (current_bytes > target_bytes && it != lru_list.end()) {
            int candidate_id = *it;
            BlockHeader* block = page_table[candidate_id];

            if (block->pin_count <= 0) { // Safety catch: Only evict unpinned!
                block->is_resident = false;
                block->pin_count = 0; // Reset
                current_bytes -= block->memory_footprint_bytes;
                
                // erase safely returns the next valid iterator
                it = lru_list.erase(it);
            } else {
                ++it; // Block is pinned (in use by AVX), skip it!
            }
        }
    }

public:
    // ---------------------------------------------------------
    // THE SINGLETON ACCESSOR (Fixes C2039)
    // ---------------------------------------------------------
    static BufferPool* get_instance() {
        static BufferPool instance; 
        return &instance;
    }

    void init(size_t max_capacity) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        max_bytes = max_capacity;
        current_bytes = 0;
        page_table.clear();
        pointer_to_page.clear();
        lru_list.clear();
    }

    void destroy() {
        std::lock_guard<std::mutex> lock(pool_mutex);
        max_bytes = 0;
        current_bytes = 0;
        page_table.clear();
        pointer_to_page.clear();
        lru_list.clear();
    }

    // =========================================================
    // REAL ENGINE HOOKS (For storage.cpp & scan.cpp)
    // =========================================================
    
    void register_block(BlockHeader* block, size_t footprint) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        
        if (max_bytes > 0 && (current_bytes + footprint) * 100 / max_bytes >= 95) {
            evict_if_needed(75);
        }

        int pid = next_page_id++;
        block->page_id = pid;
        block->is_resident = true;
        block->pin_count = 0;
        block->memory_footprint_bytes = footprint;

        page_table[pid] = block;
        pointer_to_page[block] = pid;
        
        lru_list.push_back(pid);
        current_bytes += footprint;
    }

    void pin_block(BlockHeader* block) {
        if (!block) return;
        std::lock_guard<std::mutex> lock(pool_mutex);
        
        auto it = pointer_to_page.find(block);
        if (it != pointer_to_page.end()) {
            int pid = it->second;
            
            // If it was evicted to disk, fault it back into RAM
            if (!block->is_resident) {
                if (max_bytes > 0 && (current_bytes + block->memory_footprint_bytes) * 100 / max_bytes >= 95) {
                    evict_if_needed(75); // Block until space is freed
                }
                block->is_resident = true;
                current_bytes += block->memory_footprint_bytes;
                lru_list.push_back(pid);
            } else {
                // Already in RAM, update LRU
                lru_list.remove(pid);
                lru_list.push_back(pid);
            }
            
            block->pin_count++;
        }
    }

    void unpin_block(BlockHeader* block) {
        if (!block) return;
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (block->pin_count > 0) {
            block->pin_count--;
        }
    }

    // =========================================================
    // TDD HARNESS HOOKS (For Scala Tests)
    // =========================================================

    BlockHeader* request_page(int page_id) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        if (page_table.count(page_id) && page_table[page_id]->is_resident) {
            lru_list.remove(page_id);
            lru_list.push_back(page_id); 
            return page_table[page_id];
        }

        if (max_bytes > 0 && (current_bytes + 4096) * 100 / max_bytes >= 95) {
            evict_if_needed(75); 
        }

        if (!page_table.count(page_id)) {
            page_table[page_id] = new BlockHeader();
            page_table[page_id]->memory_footprint_bytes = 4096;
        }
        
        BlockHeader* block = page_table[page_id];
        block->page_id = page_id;
        block->is_resident = true;
        block->pin_count = 0; 
        
        lru_list.push_back(page_id); 
        current_bytes += block->memory_footprint_bytes;

        return block;
    }

    void pin_page(int page_id) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (page_table.count(page_id)) {
            if (!page_table[page_id]->is_resident) {
                if (max_bytes > 0 && (current_bytes + page_table[page_id]->memory_footprint_bytes) * 100 / max_bytes >= 95) {
                    evict_if_needed(75);
                }
                page_table[page_id]->is_resident = true;
                current_bytes += page_table[page_id]->memory_footprint_bytes;
                lru_list.push_back(page_id);
            }
            page_table[page_id]->pin_count++;
        }
    }

    void unpin_page(int page_id) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (page_table.count(page_id) && page_table[page_id]->is_resident) {
            if (page_table[page_id]->pin_count > 0) {
                page_table[page_id]->pin_count--;
            }
        }
    }

    int get_usage_percent() {
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (max_bytes == 0) return 0;
        return (current_bytes * 100) / max_bytes;
    }

    bool is_resident(int page_id) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        return page_table.count(page_id) && page_table[page_id]->is_resident;
    }

    void trigger_pacemaker_sweep() {
        std::lock_guard<std::mutex> lock(pool_mutex);
        evict_if_needed(20);
    }

    // =========================================================
    // DDL LIFECYCLE HOOKS (For DROP TABLE)
    // =========================================================
    void deregister_block(BlockHeader* block) {
        if (!block) return;
        std::lock_guard<std::mutex> lock(pool_mutex);
        
        auto it = pointer_to_page.find(block);
        if (it != pointer_to_page.end()) {
            int pid = it->second;
            
            // If the block is currently in RAM, reclaim the capacity
            if (block->is_resident) {
                current_bytes -= block->memory_footprint_bytes;
            }
            
            // Erase all traces of the block from the Buffer Pool
            lru_list.remove(pid);
            page_table.erase(pid);
            pointer_to_page.erase(block);
        }
    }
};

// ---------------------------------------------------------
// RAII SCOPED PIN GUARD (Zero-Leak Execution)
// ---------------------------------------------------------
struct ScopedPin {
    BlockHeader* block;

    // Constructor: Automatically pins the block when created
    ScopedPin(BlockHeader* b) : block(b) {
        if (block) {
            BufferPool::get_instance()->pin_block(block);
        }
    }

    // Destructor: Automatically unpins the block when it goes out of scope
    ~ScopedPin() {
        if (block) {
            BufferPool::get_instance()->unpin_block(block);
        }
    }

    // Disable copy/move to prevent accidental double-unpins
    ScopedPin(const ScopedPin&) = delete;
    ScopedPin& operator=(const ScopedPin&) = delete;
};