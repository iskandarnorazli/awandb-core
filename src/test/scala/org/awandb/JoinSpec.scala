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
import scala.collection.mutable
import scala.util.Random

class JoinSpec extends AnyFlatSpec with Matchers {

  // SCENARIO: 50M Probe Rows, 10% Match Rate
  val BUILD_ROWS = 1_000_000
  val PROBE_ROWS = 50_000_000
  val MATCH_RATE = 0.10 
  val BATCH_SIZE = 4096 // L1 Cache Friendly

  "AwanDB Join Engine" should "demonstrate Sideways Information Passing (SIP) speedup" in {
    
    println("[Warmup] JIT compiling join paths...")
    runBenchmark(100_000, 1_000_000, warmup = true)
    
    println(s"\n=========================================================================================")
    println(s"| %-12s | %-12s | %-15s | %-15s | %-10s |".format("PROBE ROWS", "MATCH RATE", "STD JOIN (ms)", "SIP JOIN (ms)", "SPEEDUP"))
    println(s"=========================================================================================")

    // Run the massive test
    runBenchmark(BUILD_ROWS, PROBE_ROWS, warmup = false)
    
    println(s"=========================================================================================\n")
  }

  def runBenchmark(buildCount: Int, probeCount: Int, warmup: Boolean): Unit = {
    val random = new Random(42)
    
    // 1. GENERATE DATA
    val buildKeys = Array.range(0, buildCount)
    val maxId = (buildCount / MATCH_RATE).toInt
    val probeKeys = new Array[Int](probeCount)
    var i = 0
    while (i < probeCount) {
      probeKeys(i) = random.nextInt(maxId)
      i += 1
    }

    // 2. SETUP STRUCTURES
    // A. Standard Hash Map (JVM)
    val buildMap = new mutable.HashMap[Int, Byte](buildCount, 0.75) // Explicit load factor
    var b = 0
    while (b < buildCount) {
      buildMap.put(buildKeys(b), 1)
      b += 1
    }

    // B. Cuckoo Filter (Native)
    val cuckooPtr = NativeBridge.cuckooCreate(buildCount)
    val buildNativePtr = NativeBridge.allocMainStore(buildCount)
    NativeBridge.loadData(buildNativePtr, buildKeys)
    NativeBridge.cuckooBuildBatch(cuckooPtr, buildKeys) 

    // 3. STANDARD HASH JOIN
    val t0 = System.nanoTime()
    var stdMatches = 0L
    var p = 0
    while (p < probeCount) {
      val key = probeKeys(p)
      // Expensive: Random RAM Access (Cache Miss)
      if (buildMap.contains(key)) {
        stdMatches += 1
      }
      p += 1
    }
    val stdTime = (System.nanoTime() - t0) / 1e6

    // 4. SIP JOIN (Vectorized)
    // We process in batches to eliminate JNI overhead
    val t1 = System.nanoTime()
    var sipMatches = 0L
    p = 0
    
    // Pre-allocate batch buffers (reuse to avoid GC)
    val inputBuffer = new Array[Int](BATCH_SIZE)
    val inputPtr = NativeBridge.allocMainStore(BATCH_SIZE)
    val resultPtr = NativeBridge.allocMainStore(BATCH_SIZE)
    val resultBuffer = new Array[Int](BATCH_SIZE) // 0 or 1
    
    while (p < probeCount) {
      val len = math.min(BATCH_SIZE, probeCount - p)
      
      // A. Load Batch (Copy Java -> C++)
      // Using System.arraycopy is extremely fast (intrinsic)
      System.arraycopy(probeKeys, p, inputBuffer, 0, len)
      NativeBridge.loadData(inputPtr, inputBuffer) 
      
      // B. Native Filter (SIMD/prefetch in C++)
      NativeBridge.cuckooProbeBatch(cuckooPtr, inputPtr, len, resultPtr)
      
      // C. Read Results (Copy C++ -> Java)
      NativeBridge.copyToScala(resultPtr, resultBuffer, len)
      
      // D. Filtered Probe
      // Only check the HashMap if the bit is set
      var k = 0
      while (k < len) {
        if (resultBuffer(k) == 1) {
           // We only pay the HashMap cost for the ~10% of matching rows
           if (buildMap.contains(inputBuffer(k))) {
             sipMatches += 1
           }
        }
        k += 1
      }
      p += len
    }
    val sipTime = (System.nanoTime() - t1) / 1e6

    // Cleanup
    NativeBridge.cuckooDestroy(cuckooPtr)
    NativeBridge.freeMainStore(buildNativePtr)
    NativeBridge.freeMainStore(inputPtr)
    NativeBridge.freeMainStore(resultPtr)

    if (!warmup) {
      val speedup = stdTime / sipTime
      println(s"| %-12s | %-12s | %15.2f | %15.2f | %9.2fx |".format(
        if(probeCount > 1000000) s"${probeCount/1000000}M" else s"${probeCount/1000}K",
        s"${(MATCH_RATE*100).toInt}%",
        stdTime, 
        sipTime, 
        speedup
      ))
      
      stdMatches shouldBe sipMatches
      speedup should be > 1.2
    }
  }
}