/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#ifndef CUCKOO_H
#define CUCKOO_H

#include <cstdint>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <new>

// --- PREFETCH PORTABILITY WRAPPER ---
#if defined(_MSC_VER)
    #include <xmmintrin.h> // Required for _mm_prefetch on MSVC
    #define PREFETCH_READ(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#else
    // GCC / Clang (Linux, macOS, ARM) use builtin
    // 0 = Read, 3 = High Temporal Locality (keep in cache)
    #define PREFETCH_READ(ptr) __builtin_prefetch((const void*)(ptr), 0, 3)
#endif

#define BUCKET_SIZE 4
#define MAX_KICKS 500

typedef uint16_t fingerprint_t;

struct Bucket {
    fingerprint_t slots[BUCKET_SIZE];
};

class CuckooFilter {
public:
    size_t num_buckets;
    Bucket* buckets;
    size_t count;
    
    // Thread-local RNG state (no global locks)
    uint32_t rng_state;

    CuckooFilter(size_t capacity) {
        num_buckets = 1;
        while (num_buckets < (capacity / BUCKET_SIZE)) {
            num_buckets <<= 1;
        }
        if (num_buckets == 0) num_buckets = 1;
        
        buckets = (Bucket*)calloc(num_buckets, sizeof(Bucket));
        count = 0;
        
        // Seed with arbitrary non-zero value
        rng_state = 0xDEADBEEF; 
    }

    ~CuckooFilter() {
        if (buckets) free(buckets);
    }

    // XORSHIFT RNG (Generates random numbers in 1 CPU cycle)
    inline uint32_t fast_rand() {
        uint32_t x = rng_state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng_state = x;
        return x;
    }

    // Thomas Wang / Murmur mix
    static inline uint32_t hash(int32_t key) {
        uint32_t k = (uint32_t)key;
        k ^= k >> 16;
        k *= 0x85ebca6b;
        k ^= k >> 13;
        k *= 0xc2b2ae35;
        k ^= k >> 16;
        return k;
    }

    static inline fingerprint_t get_fingerprint(int32_t key) {
        uint32_t h = hash(key ^ 0x5bd1e995); 
        fingerprint_t fp = (fingerprint_t)(h & 0xFFFF);
        return (fp == 0) ? 1 : fp; 
    }

    // [OPTIMIZATION] Batch Insert with Cross-Platform Prefetching
    // Pipelining memory requests hides RAM latency (the biggest bottleneck).
    void insert_batch(int32_t* keys, size_t n) {
        const int PREFETCH_DIST = 16;
        
        for (size_t i = 0; i < n; i++) {
            // 1. Prefetch Future Bucket
            // We look ahead 'PREFETCH_DIST' items and pull that bucket into CPU Cache.
            if (i + PREFETCH_DIST < n) {
                int32_t next_key = keys[i + PREFETCH_DIST];
                uint32_t h = hash(next_key);
                uint32_t idx = h & (num_buckets - 1);
                
                // USE PORTABLE MACRO
                PREFETCH_READ(&buckets[idx]);
            }
            
            // 2. Insert Current
            // By the time we reach this key, the bucket should be in L1 Cache.
            insert(keys[i]);
        }
    }

    bool insert(int32_t key) {
        fingerprint_t fp = get_fingerprint(key);
        
        uint32_t i1 = hash(key) & (num_buckets - 1);
        uint32_t i2 = (i1 ^ hash(fp)) & (num_buckets - 1);

        // 1. Try Bucket 1
        for (int i = 0; i < BUCKET_SIZE; i++) {
            if (buckets[i1].slots[i] == 0) {
                buckets[i1].slots[i] = fp;
                count++;
                return true;
            }
        }

        // 2. Try Bucket 2
        for (int i = 0; i < BUCKET_SIZE; i++) {
            if (buckets[i2].slots[i] == 0) {
                buckets[i2].slots[i] = fp;
                count++;
                return true;
            }
        }

        // 3. Kick (Relocate)
        // Use fast_rand() and bitwise ops instead of modulo
        uint32_t curr_idx = (fast_rand() & 1) ? i1 : i2;
        
        for (int k = 0; k < MAX_KICKS; k++) {
            // Pick a random slot (0..3) using bitwise AND
            int slot = fast_rand() & 3; 
            
            fingerprint_t kicked_fp = buckets[curr_idx].slots[slot];
            buckets[curr_idx].slots[slot] = fp;
            fp = kicked_fp;
            
            curr_idx = (curr_idx ^ hash(fp)) & (num_buckets - 1);

            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (buckets[curr_idx].slots[i] == 0) {
                    buckets[curr_idx].slots[i] = fp;
                    count++;
                    return true;
                }
            }
        }
        return false; 
    }

    bool contains(int32_t key) {
        fingerprint_t fp = get_fingerprint(key);
        uint32_t i1 = hash(key) & (num_buckets - 1);
        
        // SIMD candidate (future optimization)
        for (int i = 0; i < BUCKET_SIZE; i++) {
            if (buckets[i1].slots[i] == fp) return true;
        }

        uint32_t i2 = (i1 ^ hash(fp)) & (num_buckets - 1);
        
        for (int i = 0; i < BUCKET_SIZE; i++) {
            if (buckets[i2].slots[i] == fp) return true;
        }
        return false;
    }

    // [PERSISTENCE] Dump raw buckets to disk
    bool save(const char* path) {
        FILE* f = fopen(path, "wb");
        if (!f) return false;
        
        fwrite(&num_buckets, sizeof(size_t), 1, f);
        fwrite(&count, sizeof(size_t), 1, f);
        fwrite(buckets, sizeof(Bucket), num_buckets, f);
        
        fclose(f);
        return true;
    }

    // [PERSISTENCE] Load raw buckets from disk
    static CuckooFilter* load(const char* path) {
        FILE* f = fopen(path, "rb");
        if (!f) return nullptr;

        size_t n_buckets, n_count;
        if (fread(&n_buckets, sizeof(size_t), 1, f) != 1) { fclose(f); return nullptr; }
        if (fread(&n_count, sizeof(size_t), 1, f) != 1) { fclose(f); return nullptr; }

        CuckooFilter* filter = new (std::nothrow) CuckooFilter(1); 
        if (!filter) { fclose(f); return nullptr; }

        if (filter->buckets) free(filter->buckets);
        filter->num_buckets = n_buckets;
        filter->count = n_count;
        filter->buckets = (Bucket*)calloc(n_buckets, sizeof(Bucket));

        if (!filter->buckets) { delete filter; fclose(f); return nullptr; }

        fread(filter->buckets, sizeof(Bucket), n_buckets, f);
        
        // Reset RNG state on load
        filter->rng_state = 0xDEADBEEF;
        
        fclose(f);
        return filter;
    }

    // Optimized for Vectorized Processing (Phase 5)
    // Processes 'count' keys in a tight loop, writing 1 or 0 to 'outResults'
    void contains_batch(int32_t* keys, size_t count, uint8_t* outResults) {
        for (size_t i = 0; i < count; i++) {
            // "this->contains" calls the existing single-key check
            outResults[i] = this->contains(keys[i]) ? 1 : 0;
        }
    }
};

#endif