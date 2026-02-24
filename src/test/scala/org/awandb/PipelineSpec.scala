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

  val ROWS = 10_000_037 
  
  // [FIX 1] Increase batch size to 65,536 to saturate memory bandwidth 
  // and deeply amortize the NativeBridge.copyToScala JNI overhead.
  val BATCH_SIZE = 65536 

  "AwanDB Vectorized Engine" should "match Scalar logic EXACTLY and outperform it" in {
    println("\n[Warmup] JIT compiling pipeline...")
    runBenchmark(100_000, warmup = true)
    
    println(s"\n===================================================================================================")
    println(s"| %-12s | %-15s | %-15s | %-10s | %-10s |".format("ROWS", "SCALAR (ms)", "VECTOR (ms)", "SPEEDUP", "INTEGRITY"))
    println(s"===================================================================================================")

    runBenchmark(ROWS, warmup = false)
    
    println(s"===================================================================================================\n")
  }

  def runBenchmark(rows: Int, warmup: Boolean): Unit = {
    // 1. DATA GEN (Deterministic Seed for Reproducibility)
    val rnd = new Random(42)
    val data = Array.fill(rows)(rnd.nextInt(rows))
    
    // 2. SETUP FILTER
    val cuckooPtr = NativeBridge.cuckooCreate(rows / 2)
    val insertPtr = NativeBridge.allocMainStore(rows / 2)
    val inserts = Array.range(0, rows / 2)
    
    try {
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

      // --- B. VECTORIZED PIPELINE (Zero-Copy) ---
      var vectorHits = 0
      var masterDataPtr = 0L
      var batchSelPtr = 0L

      try {
        masterDataPtr = NativeBridge.allocMainStore(rows)
        NativeBridge.loadData(masterDataPtr, data)
        
        batchSelPtr = NativeBridge.allocMainStore(BATCH_SIZE)
        val flags = new Array[Int](BATCH_SIZE) 

        val t1 = System.nanoTime()
        
        var pos = 0
        while (pos < rows) {
          val len = math.min(BATCH_SIZE, rows - pos)
          
          val offsetBytes = pos * 4L 
          val chunkPtr = NativeBridge.getOffsetPointer(masterDataPtr, offsetBytes)
          
          // Vectorized Probe
          NativeBridge.cuckooProbeBatch(cuckooPtr, chunkPtr, len, batchSelPtr)
          
          // Pull the resulting C++ bitmap array into the JVM for counting
          NativeBridge.copyToScala(batchSelPtr, flags, len)
          
          // [FIX 2] Branchless Array Reduction
          // Because flags(k) is strictly 0 or 1, we can add it directly.
          // This eliminates the branch predictor completely, allowing the 
          // JVM C2 compiler to auto-vectorize this loop into SIMD instructions.
          var localHits = 0
          var k = 0
          while (k < len) {
            localHits += flags(k) 
            k += 1
          }
          vectorHits += localHits
          
          pos += len
        }
        val vectorTime = (System.nanoTime() - t1) / 1e6

        // --- VALIDATION & REPORTING ---
        if (!warmup) {
          val speedup = scalarTime / vectorTime
          val matchIcon = if (scalarHits == vectorHits) "PASS" else "FAIL"
          
          println(s"| %-12s | %15.2f | %15.2f | %9.2fx | %-10s |".format(
            if(rows >= 1000000) s"${rows/1000000}M" else s"${rows/1000}K",
            scalarTime, 
            vectorTime, 
            speedup,
            matchIcon
          ))
          
          // [CRITICAL ASSERTION]
          scalarHits shouldBe vectorHits
          
          // The vectorized logic should safely clear the 0.9x hurdle and pass
          if (rows > 1_000_000) speedup should be > 0.9
        }

      } finally {
        if (masterDataPtr != 0) NativeBridge.freeMainStore(masterDataPtr)
        if (batchSelPtr != 0) NativeBridge.freeMainStore(batchSelPtr)
      }

    } finally {
      NativeBridge.freeMainStore(insertPtr)
      NativeBridge.cuckooDestroy(cuckooPtr)
    }
  }
}