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

#include "common.h"
#include <cstring>

extern "C" {

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_csrBfsNative(
        JNIEnv* env, jobject obj, jlong rowPtrsAddr, jlong colIdxsAddr, jint numVertices, jint startNode, jlong outDistancesAddr
    ) {
        if (rowPtrsAddr == 0 || colIdxsAddr == 0 || outDistancesAddr == 0) return;

        // [ZERO-COPY] Cast the raw memory pointers safely
        int32_t* rowPtrs = (int32_t*)rowPtrsAddr;
        int32_t* colIdxs = (int32_t*)colIdxsAddr;
        int32_t* distances = (int32_t*)outDistancesAddr;

        // 1. Initialize all distances to -1 (Unreachable)
        // Vectorized by the compiler via memset equivalent
        for (int i = 0; i < numVertices; i++) {
            distances[i] = -1;
        }

        // Bounds check
        if (startNode < 0 || startNode >= numVertices) return;

        // 2. Pre-allocate the Frontier Queue (Zero dynamic allocations in the loop)
        // We use alloc_aligned to match the alignment of the rest of the engine
        int32_t* queue = (int32_t*)alloc_aligned(numVertices * sizeof(int32_t));
        int head = 0;
        int tail = 0;

        // 3. Setup Start Node
        distances[startNode] = 0;
        queue[tail++] = startNode;

        // 4. Execute Native BFS
        while (head < tail) {
            int32_t current = queue[head++];
            int32_t current_dist = distances[current];

            // CSR Lookup: Find where this node's edges start and end in the colIdxs array
            int32_t edge_start = rowPtrs[current];
            int32_t edge_end = rowPtrs[current + 1];

            // Iterate over all outgoing edges
            for (int32_t e = edge_start; e < edge_end; e++) {
                int32_t neighbor = colIdxs[e];
                
                // If unvisited, set distance and push to queue
                if (distances[neighbor] == -1) {
                    distances[neighbor] = current_dist + 1;
                    queue[tail++] = neighbor;
                }
            }
        }

        // Cleanup the temporary queue
        free_aligned(queue);
    }

}