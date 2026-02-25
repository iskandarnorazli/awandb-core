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
#include <algorithm>
#include <vector>
#include <thread>
#include <atomic>

// Thresholds for switching algorithms
#define PARALLEL_THRESHOLD 1000000 
#define SMALL_BUCKET_THRESHOLD 64

// ---------------------------------------------------------
// SEQUENTIAL FALLBACKS
// ---------------------------------------------------------

void insertion_sort(int* ptr, int count) {
    for (int i = 1; i < count; i++) {
        int key = ptr[i];
        int j = i - 1;
        while (j >= 0 && ptr[j] > key) {
            ptr[j + 1] = ptr[j];
            j--;
        }
        ptr[j + 1] = key;
    }
}

// Single-threaded recursive MSD Radix Sort
// [Optimization] Uses pre-allocated aux buffer to avoid excessive mallocs
void radix_msd_recursive(int* ptr, int* aux, int count, int shift) {
    if (count <= SMALL_BUCKET_THRESHOLD) {
        insertion_sort(ptr, count);
        return;
    }

    int counts[256] = {0};
    
    // 1. Histogram (Count frequencies)
    for (int i = 0; i < count; i++) {
        int bucket = (ptr[i] >> shift) & 0xFF;
        if (shift == 24) bucket ^= 128; // Handle signed integers correctly
        counts[bucket]++;
    }

    // 2. Calculate Offsets (Prefix Sum)
    int heads[256];
    int offset = 0;
    for (int i = 0; i < 256; i++) {
        heads[i] = offset;
        offset += counts[i];
    }

    // 3. Shuffle to Aux (Scatter)
    // We need a mutable copy of heads
    int write_heads[256];
    std::memcpy(write_heads, heads, 256 * sizeof(int));

    for (int i = 0; i < count; i++) {
        int val = ptr[i];
        int bucket = (val >> shift) & 0xFF;
        if (shift == 24) bucket ^= 128;
        aux[write_heads[bucket]++] = val;
    }

    // 4. Copy back to original array
    std::memcpy(ptr, aux, count * sizeof(int));

    // 5. Recurse (if not the last byte)
    if (shift >= 8) {
        for (int i = 0; i < 256; i++) {
            int bucket_size = counts[i];
            if (bucket_size > 1) {
                // Recursive call on the sub-bucket
                radix_msd_recursive(ptr + heads[i], aux + heads[i], bucket_size, shift - 8);
            }
        }
    }
}

// ---------------------------------------------------------
// PARALLEL RADIX SORT ENGINE
// ---------------------------------------------------------

void parallel_radix_msd(int* ptr, int count) {
    // 1. Memory Allocation
    // Allocate one giant auxiliary buffer for the whole sort operation.
    // This is faster than allocating small buffers in every recursive step.
    int* aux = (int*)alloc_aligned(count * sizeof(int));
    if (!aux) return;

    // 2. Hardware Discovery
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
    if (num_threads > 64) num_threads = 64;
    
    // 3. Phase 1: Parallel Histogram
    // We split the array into chunks, one for each thread.
    int chunk_size = (count + num_threads - 1) / num_threads;
    std::vector<std::vector<int>> local_histograms(num_threads, std::vector<int>(256, 0));
    std::vector<std::thread> workers;

    for (unsigned int t = 0; t < num_threads; t++) {
        workers.emplace_back([&, t]() {
            int start = t * chunk_size;
            int end = std::min(start + chunk_size, count);
            if (start >= end) return;

            for (int i = start; i < end; i++) {
                int bucket = (ptr[i] >> 24) & 0xFF;
                bucket ^= 128; // Signed int toggle
                local_histograms[t][bucket]++;
            }
        });
    }
    for (auto& t : workers) t.join();
    workers.clear();

    // 4. Phase 2: Global Prefix Sum
    // Calculate exactly where each thread should write each bucket.
    // This allows threads to write to 'aux' simultaneously without locks.
    int global_heads[256] = {0};
    int global_offsets[256][64]; // [bucket][thread_id] -> start index in aux
    
    int current_global_pos = 0;
    for (int b = 0; b < 256; b++) {
        global_heads[b] = current_global_pos;
        for (unsigned int t = 0; t < num_threads; t++) {
            global_offsets[b][t] = current_global_pos;
            current_global_pos += local_histograms[t][b];
        }
    }

    // 5. Phase 3: Parallel Shuffle (The Heavy Lifting)
    for (unsigned int t = 0; t < num_threads; t++) {
        workers.emplace_back([&, t]() {
            int start = t * chunk_size;
            int end = std::min(start + chunk_size, count);
            if (start >= end) return;

            // Make local copies of write offsets to avoid cache thrashing (False Sharing)
            int local_write_heads[256];
            for (int b = 0; b < 256; b++) {
                local_write_heads[b] = global_offsets[b][t];
            }

            for (int i = start; i < end; i++) {
                int val = ptr[i];
                int bucket = (val >> 24) & 0xFF;
                bucket ^= 128;
                
                // Write directly to the globally calculated position
                aux[local_write_heads[bucket]++] = val;
            }
        });
    }
    for (auto& t : workers) t.join();
    workers.clear();

    // 6. Phase 4: Parallel Copy Back
    // memcpy is fast, but parallel memcpy saturates RAM bandwidth better
    for (unsigned int t = 0; t < num_threads; t++) {
        workers.emplace_back([&, t]() {
            int start = t * chunk_size;
            int end = std::min(start + chunk_size, count);
            if (start < end) {
                std::memcpy(ptr + start, aux + start, (end - start) * sizeof(int));
            }
        });
    }
    for (auto& t : workers) t.join();
    workers.clear();

    // 7. Phase 5: Parallel Recursion
    // We have 256 buckets to sort. We distribute these buckets to our threads.
    // Use an atomic counter to hand out buckets dynamically (Load Balancing).
    
    std::atomic<int> bucket_idx(0);
    
    for (unsigned int t = 0; t < num_threads; t++) {
        workers.emplace_back([&]() {
            while (true) {
                // Claim the next available bucket
                int b = bucket_idx.fetch_add(1);
                if (b >= 256) break;

                // Calculate bucket size
                int size = 0;
                for(unsigned int i=0; i<num_threads; i++) size += local_histograms[i][b];
                
                if (size > 1) {
                    // For the next level, we use the single-threaded recursive sort
                    // because the chunks are now small enough to fit in L2 cache.
                    // We malloc a small temp buffer for this bucket's recursion.
                    int* bucket_aux = (int*)malloc(size * sizeof(int));
                    if (bucket_aux) {
                        radix_msd_recursive(ptr + global_heads[b], bucket_aux, size, 16); // shift 16 = next byte
                        free(bucket_aux);
                    }
                }
            }
        });
    }
    for (auto& t : workers) t.join();

    free_aligned(aux);
}

extern "C" {
    // 1. AUTO-SWITCHING SORT (Parallel for large, Single for small)
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_radixSortNative(JNIEnv* env, jobject obj, jlong ptr, jint count) {
        if (ptr == 0 || count <= 0) return;
        int* data = (int*)ptr;
        
        // Strategy: Use Parallel Sort for big data, fallback to simple malloc for small data
        if (count >= PARALLEL_THRESHOLD) {
            parallel_radix_msd(data, count);
        } else {
            int* aux = (int*)malloc(count * sizeof(int));
            if (aux) {
                radix_msd_recursive(data, aux, count, 24);
                free(aux);
            }
        }
    }

    // 2. [NEW] FORCED SINGLE-THREADED SORT (For Benchmarking)
    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_radixSortSingleNative(JNIEnv* env, jobject obj, jlong ptr, jint count) {
        if (ptr == 0 || count <= 0) return;
        int* data = (int*)ptr;
        
        // Force single threaded regardless of size
        int* aux = (int*)malloc(count * sizeof(int));
        if (aux) {
            radix_msd_recursive(data, aux, count, 24);
            free(aux);
        }
    }
}