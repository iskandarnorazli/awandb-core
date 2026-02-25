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

package org.awandb

import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VectorPerformanceSpec extends AnyFlatSpec with Matchers {

  "AwanDB Vector Engine" should "benchmark Cosine Similarity Throughput" in {
    val dim = 128 // Standard "Small" Embedding (e.g. quantization or smaller models)
    val rows = 1_000_000 // 1 Million Vectors
    val totalFloats = rows * dim
    
    println(s"=== Vector Benchmark: ${rows / 1_000_000}M Rows, Dim=$dim ===")
    println(s"Total Data Size: ${(totalFloats * 4) / 1024 / 1024} MB")

    // 1. Allocate (Using Int-as-Bytes hack: 128 floats = 128 ints size-wise)
    val colSizes = Array(totalFloats * 4) // 4 bytes per Float
    val blockPtr = NativeBridge.createBlock(rows, 1, colSizes) // Pass the actual 'rows' variable
    
    // 2. Generate Data (Random Normalized Floats)
    // We reuse a small buffer to fill the large array to save setup time
    val data = new Array[Float](totalFloats)
    val slice = new Array[Float](dim * 1000)
    val rnd = new scala.util.Random()
    for (i <- slice.indices) slice(i) = rnd.nextFloat()
    
    // Fill main array
    var offset = 0
    while (offset < totalFloats) {
      val copyLen = math.min(slice.length, totalFloats - offset)
      Array.copy(slice, 0, data, offset, copyLen)
      offset += copyLen
    }
    
    // Load into Engine
    NativeBridge.loadVectorData(blockPtr, 0, data, dim)
    
    // 3. Prepare Query
    val query = new Array[Float](dim)
    for (i <- 0 until dim) query(i) = rnd.nextFloat()

    // 4. Benchmark Loop
    val iterations = 50
    var totalTime = 0L
    
    // Warmup
    NativeBridge.avxScanVectorCosine(blockPtr, 0, query, 0.99f)

    println("| Run | Time (ms) | Speed (GB/s) |")
    println("|-----|-----------|--------------|")

    for (i <- 1 to iterations) {
      val t0 = System.nanoTime()
      NativeBridge.avxScanVectorCosine(blockPtr, 0, query, 0.99f)
      val t1 = System.nanoTime()
      val ms = (t1 - t0) / 1e6
      
      // GB/s = (Total Bytes) / (Time in Seconds)
      val gbs = ((totalFloats * 4L) / 1e9) / ((t1 - t0) / 1e9)
      
      if (i % 10 == 0) println(f"| $i%3d | $ms%9.2f | $gbs%12.2f |")
      totalTime += (t1 - t0)
    }

    val avgTime = totalTime / iterations / 1e6
    val avgGbs = ((totalFloats * 4L) / 1e9) / (totalTime / iterations / 1e9)
    
    println("==================================")
    println(f"AVG Throughput: $avgGbs%.2f GB/s")
    println("==================================")

    NativeBridge.freeMainStore(blockPtr)
  }
}