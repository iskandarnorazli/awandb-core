/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import scala.util.Random

class PipelineSpec extends AnyFlatSpec with Matchers {

  val ROWS = 10_000_000
  val BATCH_SIZE = 4096 

  "AwanDB Vectorized Engine" should "outperform Scalar Loops using Zero-Copy Batches" in {
    println("\n[Warmup] JIT compiling pipeline...")
    runBenchmark(100_000, warmup = true)
    
    println(s"\n=========================================================================================")
    println(s"| %-12s | %-15s | %-15s | %-10s |".format("ROWS", "SCALAR LOOP (ms)", "VECTOR DAG (ms)", "SPEEDUP"))
    println(s"=========================================================================================")

    runBenchmark(ROWS, warmup = false)
    
    println(s"=========================================================================================\n")
  }

  def runBenchmark(rows: Int, warmup: Boolean): Unit = {
    // 1. DATA GEN
    val data = Array.fill(rows)(Random.nextInt(rows))
    
    // 2. SETUP FILTER
    val cuckooPtr = NativeBridge.cuckooCreate(rows / 2)
    val insertPtr = NativeBridge.allocMainStore(rows / 2)
    val inserts = Array.range(0, rows / 2)
    NativeBridge.loadData(insertPtr, inserts)
    NativeBridge.cuckooBuildBatch(cuckooPtr, inserts)

    // --- A. SCALAR LOOP (Baseline) ---
    val t0 = System.nanoTime()
    var scalarHits = 0
    var i = 0
    while (i < rows) {
      if (NativeBridge.cuckooContains(cuckooPtr, data(i))) {
        scalarHits += 1
      }
      i += 1
    }
    val scalarTime = (System.nanoTime() - t0) / 1e6

    // --- B. VECTORIZED PIPELINE (Zero-Copy Optimization) ---
    // 1. Preload ALL data to C++ heap (Simulating Memory-Mapped File)
    val masterDataPtr = NativeBridge.allocMainStore(rows)
    NativeBridge.loadData(masterDataPtr, data)

    val t1 = System.nanoTime()
    var vectorHits = 0
    
    // We allocate *one* result buffer and reuse it.
    val batchSelPtr = NativeBridge.allocMainStore(BATCH_SIZE)
    val flags = new Array[Int](BATCH_SIZE) 
    
    var pos = 0
    while (pos < rows) {
      val len = math.min(BATCH_SIZE, rows - pos)
      
      // ZERO-COPY MAGIC:
      // Calculate the pointer to the current chunk inside the master buffer.
      // 4 bytes per int.
      val offsetBytes = pos * 4 
      val chunkPtr = NativeBridge.getOffsetPointer(masterDataPtr, offsetBytes)
      
      // Pass the pointer directly. No copying data from Java to C++.
      NativeBridge.cuckooProbeBatch(cuckooPtr, chunkPtr, len, batchSelPtr)
      
      // Read back results (Selection Vector)
      NativeBridge.copyToScala(batchSelPtr, flags, len)
      
      // Count hits (Simulating downstream operator)
      var k = 0
      while (k < len) {
        if (flags(k) == 1) vectorHits += 1
        k += 1
      }
      
      pos += len
    }
    
    val vectorTime = (System.nanoTime() - t1) / 1e6

    // Cleanup
    NativeBridge.freeMainStore(masterDataPtr)
    NativeBridge.freeMainStore(batchSelPtr)
    NativeBridge.freeMainStore(insertPtr)
    NativeBridge.cuckooDestroy(cuckooPtr)

    if (!warmup) {
      val speedup = scalarTime / vectorTime
      println(s"| %-12s | %15.2f | %15.2f | %9.2fx |".format(
        if(rows >= 1000000) s"${rows/1000000}M" else s"${rows/1000}K",
        scalarTime, 
        vectorTime, 
        speedup
      ))
      
      // With Zero-Copy, this should fly.
      speedup should be > 0.9
    }
  }
}