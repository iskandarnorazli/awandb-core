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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb

import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable
import scala.util.Random

class JoinSpec extends AnyFlatSpec with Matchers {

  // SCENARIO:
  // "Build Side" (Users) = 1 Million Rows (The small table)
  // "Probe Side" (Logs)  = 50 Million Rows (The huge table)
  val BUILD_ROWS = 1_000_000
  val PROBE_ROWS = 50_000_000
  
  // Selectivity: Only 10% of logs actually match a user in the build side
  val MATCH_RATE = 0.10 

  "AwanDB Join Engine" should "demonstrate Sideways Information Passing (SIP) speedup" in {
    
    // [JIT Warmup]
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
    // [FIX] Added loadFactor 0.75 to constructor
    val buildMap = new mutable.HashMap[Int, Byte](buildCount, 0.75)
    var b = 0
    while (b < buildCount) {
      buildMap.put(buildKeys(b), 1)
      b += 1
    }

    // B. Cuckoo Filter (The SIP Filter)
    val cuckooPtr = NativeBridge.cuckooCreate(buildCount)
    val buildNativePtr = NativeBridge.allocMainStore(buildCount)
    NativeBridge.loadData(buildNativePtr, buildKeys)
    NativeBridge.cuckooBuildBatch(cuckooPtr, buildKeys) // Bulk Insert

    // 3. STANDARD HASH JOIN
    val t0 = System.nanoTime()
    var stdMatches = 0L
    var p = 0
    while (p < probeCount) {
      val key = probeKeys(p)
      if (buildMap.contains(key)) {
        stdMatches += 1
      }
      p += 1
    }
    val stdTime = (System.nanoTime() - t0) / 1e6

    // 4. SIP JOIN (Cuckoo Filtered)
    val t1 = System.nanoTime()
    var sipMatches = 0L
    p = 0
    while (p < probeCount) {
      val key = probeKeys(p)
      
      // 1. CHEAP CHECK: Native Cuckoo Filter
      // This stays in CPU Cache. No random RAM access.
      if (NativeBridge.cuckooContains(cuckooPtr, key)) {
        
        // 2. EXPENSIVE CHECK: Hash Map
        // Only executed for the 10% that match
        if (buildMap.contains(key)) {
          sipMatches += 1
        }
      }
      p += 1
    }
    val sipTime = (System.nanoTime() - t1) / 1e6

    // Cleanup
    NativeBridge.cuckooDestroy(cuckooPtr)
    NativeBridge.freeMainStore(buildNativePtr)

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
      if (probeCount >= 1_000_000) speedup should be > 1.2
    }
  }
}