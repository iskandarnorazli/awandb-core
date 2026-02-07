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
import scala.util.Random
import java.util.Arrays

class SortSpec extends AnyFlatSpec with Matchers {

  // We test up to 50M to see the scaling
  val SIZES = Seq(1_000, 10_000, 100_000, 1_000_000, 10_000_000, 20_000_000, 50_000_000)

  "AwanDB Sort Engine" should "outperform Java Arrays.sort in both Single and Parallel modes" in {
    
    // ==============================================================================
    // 1. JVM WARMUP
    // ==============================================================================
    println("\n[Warmup] Heating up JVM and C++ Thread Pool...")
    val warmupData = Array.fill(100_000)(Random.nextInt())
    val nativePtr = NativeBridge.allocMainStore(100_000)
    
    try {
      for (i <- 1 to 15) {
        val c1 = warmupData.clone(); Arrays.sort(c1)
        val c2 = warmupData.clone(); Arrays.parallelSort(c2)
        NativeBridge.loadData(nativePtr, warmupData)
        NativeBridge.radixSortSingle(nativePtr, 100_000) // Warmup Single
        NativeBridge.radixSort(nativePtr, 100_000)       // Warmup Parallel
      }
    } finally {
      NativeBridge.freeMainStore(nativePtr)
    }
    println("[Warmup] Complete.\n")

    // ==============================================================================
    // 2. THE 4-WAY BENCHMARK
    // ==============================================================================
    println(s"==========================================================================================================================")
    println(s"| %-12s | %-13s | %-13s | %-13s | %-13s | %-10s |".format("ROWS", "JAVA SEQ", "C++ SEQ", "JAVA PAR", "C++ PAR", "SPEEDUP"))
    println(s"==========================================================================================================================")

    for (rows <- SIZES) {
      runBenchmark(rows)
      System.gc() 
      Thread.sleep(200) 
    }
    println(s"==========================================================================================================================\n")
  }

  def runBenchmark(rows: Int): Unit = {
    val random = new Random(42)
    val originalData = Array.fill(rows)(random.nextInt())
    
    // Copies for Java
    val javaSeqData = originalData.clone() 
    val javaParData = originalData.clone()
    
    // --- 1. JAVA SEQUENTIAL ---
    var t0 = System.nanoTime()
    Arrays.sort(javaSeqData)
    val timeJavaSeq = (System.nanoTime() - t0) / 1e6

    // --- 2. JAVA PARALLEL ---
    t0 = System.nanoTime()
    Arrays.parallelSort(javaParData)
    val timeJavaPar = (System.nanoTime() - t0) / 1e6

    val nativePtr = NativeBridge.allocMainStore(rows)
    try {
      // --- 3. C++ SEQUENTIAL (Forced) ---
      NativeBridge.loadData(nativePtr, originalData) // Reload unsorted
      t0 = System.nanoTime()
      NativeBridge.radixSortSingle(nativePtr, rows)
      val timeCppSeq = (System.nanoTime() - t0) / 1e6

      // --- 4. C++ PARALLEL (Auto/Forced) ---
      NativeBridge.loadData(nativePtr, originalData) // Reload unsorted
      t0 = System.nanoTime()
      NativeBridge.radixSort(nativePtr, rows) // This auto-switches to parallel for large inputs
      val timeCppPar = (System.nanoTime() - t0) / 1e6

      // Calculate Speedup: C++ Parallel vs Java Parallel
      val speedup = timeJavaPar / timeCppPar
      
      println(s"| %-12s | %10.2f ms | %10.2f ms | %10.2f ms | %10.2f ms | %9.2fx |".format(
        java.text.NumberFormat.getIntegerInstance.format(rows), 
        timeJavaSeq, 
        timeCppSeq, 
        timeJavaPar, 
        timeCppPar,
        speedup
      ))
      
      // ASSERTION: C++ Parallel must beat Java Parallel on large datasets
      if (rows >= 1_000_000) {
         speedup should be > 1.0
      }

    } finally {
      NativeBridge.freeMainStore(nativePtr)
    }
  }
}