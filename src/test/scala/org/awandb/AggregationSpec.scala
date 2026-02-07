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

class AggregationSpec extends AnyFlatSpec with Matchers {

  val ROWS = 50_000_000 
  
  // Test Scenarios:
  // 1. Low Cardinality (100 groups) -> Fits in CPU L1 Cache.
  // 2. High Cardinality (5M groups) -> Stresses Main RAM (Random Access).
  val CARDINALITIES = Seq(100, 5_000_000)

  "AwanDB Aggregation Engine" should "outperform Java HashMap on GROUP BY SUM" in {
    
    println("\n[Warmup] JIT compiling aggregation paths...")
    runBenchmark(100_000, 100, warmup = true)
    
    println(s"\n=========================================================================================")
    println(s"| %-12s | %-12s | %-15s | %-15s | %-10s |".format("ROWS", "GROUPS", "JAVA MAP (ms)", "C++ HASH (ms)", "SPEEDUP"))
    println(s"=========================================================================================")

    for (cardinality <- CARDINALITIES) {
      runBenchmark(ROWS, cardinality, warmup = false)
      System.gc()
      Thread.sleep(500)
    }
    println(s"=========================================================================================\n")
  }

  def runBenchmark(rows: Int, distinctKeys: Int, warmup: Boolean): Unit = {
    // 1. SETUP DATA
    val random = new Random(42)
    val keys = new Array[Int](rows)
    val values = new Array[Int](rows)
    
    // Fill arrays (simulating a column store)
    var i = 0
    while (i < rows) {
      keys(i) = random.nextInt(distinctKeys) 
      values(i) = random.nextInt(100)        
      i += 1
    }

    // 2. JAVA BASELINE (Mutable HashMap)
    val t0 = System.nanoTime()
    val javaMap = new mutable.HashMap[Int, Long](distinctKeys, 0.75) 
    
    var j = 0
    while (j < rows) {
      val k = keys(j)
      val v = values(j)
      javaMap.update(k, javaMap.getOrElse(k, 0L) + v)
      j += 1
    }
    val t1 = System.nanoTime()
    val javaTime = (t1 - t0) / 1e6

    // 3. NATIVE AGGREGATION (AwanDB)
    val keysPtr = NativeBridge.allocMainStore(rows)
    val valsPtr = NativeBridge.allocMainStore(rows)
    
    try {
      NativeBridge.loadData(keysPtr, keys)
      NativeBridge.loadData(valsPtr, values)
      
      val t2 = System.nanoTime()
      
      // The Native Kernel returns a pointer to the result map
      val resultPtr = NativeBridge.aggregateSum(keysPtr, valsPtr, rows)
      
      val t3 = System.nanoTime()
      val cppTime = (t3 - t2) / 1e6
      
      if (resultPtr != 0) NativeBridge.freeAggregationResult(resultPtr)

      if (!warmup) {
        val speedup = javaTime / cppTime
        println(s"| %-12s | %-12s | %15.2f | %15.2f | %9.2fx |".format(
          if(rows > 1000000) s"${rows/1000000}M" else s"${rows/1000}K",
          s"$distinctKeys",
          javaTime, 
          cppTime, 
          speedup
        ))
        
        if (rows >= 1_000_000) speedup should be > 1.2
      }

    } finally {
      NativeBridge.freeMainStore(keysPtr)
      NativeBridge.freeMainStore(valsPtr)
    }
  }
}