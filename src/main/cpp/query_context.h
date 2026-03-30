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

#include "common.h"
#include <vector>
#include <mutex>

class QueryContext {
private:
    std::vector<void*> allocated_pointers; 
    std::mutex alloc_mutex; // Protects vector during concurrent query allocations

public:
    QueryContext() = default;

    // [CRITICAL] Prevent copying to avoid double-free heap corruption
    QueryContext(const QueryContext&) = delete;
    QueryContext& operator=(const QueryContext&) = delete;

    void register_allocation(void* ptr) {
        if (ptr == nullptr) return; // Don't track nulls
        
        std::lock_guard<std::mutex> lock(alloc_mutex);
        allocated_pointers.push_back(ptr);
    }
    
    ~QueryContext() {
        std::lock_guard<std::mutex> lock(alloc_mutex);
        for (void* ptr : allocated_pointers) {
            if (ptr != nullptr) {
                free_aligned(ptr); 
            }
        }
        allocated_pointers.clear();
    }
};