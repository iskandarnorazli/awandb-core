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

#pragma once

#include <atomic>
#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>
#include <condition_variable> // [NEW] Added for thread blocking

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
    std::condition_variable pool_cv; // [NEW] Condition Variable to block starving threads
    std::unordered_map<int, std::list<int>::iterator> lru_iters;

    // --- SINGLETON SETUP ---
    BufferPool() = default;
    ~BufferPool() = default;
    
    // Disable copy/move
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    void evict_if_needed(size_t target_usage_percent) {
        if (max_bytes == 0) return;
        size_t target_bytes = (max_bytes * target_usage_percent) / 100;
        
        auto it = lru_list.begin();
        while (current_bytes > target_bytes && it != lru_list.end()) {
            int candidate_id = *it;
            BlockHeader* block = page_table[candidate_id];

            if (block->pin_count <= 0) { 
                block->is_resident = false;
                current_bytes -= block->memory_footprint_bytes;
                
                lru_iters.erase(candidate_id);
                it = lru_list.erase(it);

                // [CRITICAL FIX] Memory Leak & Corruption Prevention
                // Test harness blocks are NOT in pointer_to_page. 
                // We MUST delete them and erase them from the map to prevent dangling pointers.
                if (pointer_to_page.count(block) == 0) {
                    page_table.erase(candidate_id);
                    delete block; // Safely invoke C++ destructor instead of raw free()
                }
            } else {
                ++it; 
            }
        }
    }

public:
    static BufferPool* get_instance() {
        static BufferPool instance; 
        return &instance;
    }

    void init(size_t max_capacity) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        max_bytes = max_capacity;
        current_bytes = 0;
        
        // Safely wipe any lingering Test Harness blocks from previous tests
        for (auto it = page_table.begin(); it != page_table.end(); ) {
            if (pointer_to_page.count(it->second) == 0) {
                delete it->second;
                it = page_table.erase(it);
            } else {
                ++it;
            }
        }

        pointer_to_page.clear();
        lru_list.clear();
        lru_iters.clear();
        pool_cv.notify_all(); // Wake up any lingering threads
    }

    void destroy() {
        std::lock_guard<std::mutex> lock(pool_mutex);
        max_bytes = 0;
        current_bytes = 0;
        
        // Safely wipe any lingering Test Harness blocks
        for (auto it = page_table.begin(); it != page_table.end(); ) {
            if (pointer_to_page.count(it->second) == 0) {
                delete it->second;
                it = page_table.erase(it);
            } else {
                ++it;
            }
        }

        pointer_to_page.clear();
        lru_list.clear();
        lru_iters.clear();
        pool_cv.notify_all(); 
    }

    // =========================================================
    // REAL ENGINE HOOKS 
    // =========================================================
    
    void register_block(BlockHeader* block, size_t footprint) {
        std::unique_lock<std::mutex> lock(pool_mutex);
        
        while (true) {
            // 1. Check if we already have space
            if (max_bytes == 0 || (current_bytes + footprint) * 100 / max_bytes < 95) {
                break; 
            }
            
            // 2. We don't have space. Try to evict down to 75%.
            evict_if_needed(75);
            
            // 3. Check if eviction worked
            if ((current_bytes + footprint) * 100 / max_bytes < 95) {
                break; 
            }
            
            // 4. Memory is full of pinned pages. Sleep until someone unpins.
            pool_cv.wait(lock); 
        }

        int pid = next_page_id++;
        block->page_id = pid;
        block->is_resident = true;
        block->pin_count = 0;
        block->memory_footprint_bytes = footprint;

        page_table[pid] = block;
        pointer_to_page[block] = pid;
        
        lru_list.push_back(pid);
        lru_iters[pid] = std::prev(lru_list.end());
        current_bytes += footprint;
    }

    void pin_block(BlockHeader* block) {
        if (!block) return;
        std::unique_lock<std::mutex> lock(pool_mutex);
        
        auto it = pointer_to_page.find(block);
        if (it != pointer_to_page.end()) {
            int pid = it->second;
            
            while (true) {
                if (block->is_resident) {
                    auto lru_it = lru_iters.find(pid);
                    if (lru_it != lru_iters.end()) lru_list.erase(lru_it->second); 
                    lru_list.push_back(pid);
                    lru_iters[pid] = std::prev(lru_list.end());
                    break;
                }

                size_t footprint = block->memory_footprint_bytes;
                
                if (max_bytes == 0 || (current_bytes + footprint) * 100 / max_bytes < 95) {
                    block->is_resident = true;
                    current_bytes += footprint;
                    lru_list.push_back(pid);
                    lru_iters[pid] = std::prev(lru_list.end());
                    break;
                }

                evict_if_needed(75); 
                
                if ((current_bytes + footprint) * 100 / max_bytes < 95) {
                    block->is_resident = true;
                    current_bytes += footprint;
                    lru_list.push_back(pid);
                    lru_iters[pid] = std::prev(lru_list.end());
                    break;
                }
                pool_cv.wait(lock);
            }
            block->pin_count++;
        }
    }

    void unpin_block(BlockHeader* block) {
        if (!block) return;
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (block->pin_count > 0) {
            block->pin_count--;
            // [FIX] If the block is fully unpinned, it can now be evicted! Wake up sleeping threads.
            if (block->pin_count == 0) {
                pool_cv.notify_all(); 
            }
        }
    }

    // =========================================================
    // TDD HARNESS HOOKS 
    // =========================================================

    BlockHeader* request_page(int page_id) {
        std::unique_lock<std::mutex> lock(pool_mutex);

        while (true) {
            if (page_table.count(page_id) && page_table[page_id]->is_resident) {
                auto lru_it = lru_iters.find(page_id);
                if (lru_it != lru_iters.end()) lru_list.erase(lru_it->second);
                lru_list.push_back(page_id); 
                lru_iters[page_id] = std::prev(lru_list.end()); 
                return page_table[page_id];
            }

            if (max_bytes == 0 || (current_bytes + 4096) * 100 / max_bytes < 95) {
                break;
            }

            evict_if_needed(75); 
            
            if ((current_bytes + 4096) * 100 / max_bytes < 95) {
                break;
            }
            pool_cv.wait(lock);
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
        lru_iters[page_id] = std::prev(lru_list.end());
        current_bytes += block->memory_footprint_bytes;

        return block;
    }

    void pin_page(int page_id) {
        std::unique_lock<std::mutex> lock(pool_mutex);
        if (!page_table.count(page_id)) return;

        while (true) {
            if (page_table[page_id]->is_resident) {
                break;
            }
            
            size_t footprint = page_table[page_id]->memory_footprint_bytes;
            
            if (max_bytes == 0 || (current_bytes + footprint) * 100 / max_bytes < 95) {
                page_table[page_id]->is_resident = true;
                current_bytes += footprint;
                lru_list.push_back(page_id);
                lru_iters[page_id] = std::prev(lru_list.end());
                break;
            }

            evict_if_needed(75);
            
            if ((current_bytes + footprint) * 100 / max_bytes < 95) {
                page_table[page_id]->is_resident = true;
                current_bytes += footprint;
                lru_list.push_back(page_id);
                lru_iters[page_id] = std::prev(lru_list.end());
                break;
            }
            pool_cv.wait(lock);
        }
        page_table[page_id]->pin_count++;
    }

    void unpin_page(int page_id) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (page_table.count(page_id) && page_table[page_id]->is_resident) {
            if (page_table[page_id]->pin_count > 0) {
                page_table[page_id]->pin_count--;
                // [FIX] Wake up sleeping threads
                if (page_table[page_id]->pin_count == 0) {
                    pool_cv.notify_all();
                }
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
            
            if (block->is_resident) {
                current_bytes -= block->memory_footprint_bytes;
            }
            
            auto lru_it = lru_iters.find(pid);
            if (lru_it != lru_iters.end()) {
                lru_list.erase(lru_it->second);
                lru_iters.erase(lru_it);
            }
            
            page_table.erase(pid);
            pointer_to_page.erase(block);
            
            // [FIX] Memory freed, wake up waiters!
            pool_cv.notify_all();
        }
    }

    void unregister_block(BlockHeader* block) {
        deregister_block(block);
    }
};

// ---------------------------------------------------------
// RAII SCOPED PIN GUARD (Zero-Leak Execution)
// ---------------------------------------------------------
struct ScopedPin {
    BlockHeader* block;

    ScopedPin(BlockHeader* b) : block(b) {
        if (block) {
            BufferPool::get_instance()->pin_block(block);
        }
    }

    ~ScopedPin() {
        if (block) {
            BufferPool::get_instance()->unpin_block(block);
        }
    }

    ScopedPin(const ScopedPin&) = delete;
    ScopedPin& operator=(const ScopedPin&) = delete;
};