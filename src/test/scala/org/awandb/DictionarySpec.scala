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

class DictionarySpec extends AnyFlatSpec with Matchers {

  val ROWS = 1_000_000
  // Test both low cardinality (repetitive categories) and high cardinality (IDs)
  val CARDINALITIES = Seq(100, 100_000)

  "AwanDB Dictionary Engine" should "outperform Java String Sort via Encoded Radix Sort" in {
    
    println("\n[Warmup] JIT compiling dictionary paths...")
    runBenchmark(100_000, 100, warmup = true)
    
    println(s"\n=========================================================================================")
    println(s"| %-12s | %-12s | %-15s | %-15s | %-10s |".format("ROWS", "UNIQUE", "JAVA SORT (ms)", "NATIVE SORT (ms)", "SPEEDUP"))
    println(s"=========================================================================================")

    for (cardinality <- CARDINALITIES) {
      runBenchmark(ROWS, cardinality, warmup = false)
    }
    
    println(s"=========================================================================================\n")
  }

  def runBenchmark(rows: Int, distinctCount: Int, warmup: Boolean): Unit = {
    // 1. DATA GEN
    val distinctValues = (0 until distinctCount).map(i => s"val_prefix_${i}").toArray
    val rawStrings = Array.fill(rows)(distinctValues(Random.nextInt(distinctCount)))
    
    val dictPtr = NativeBridge.dictionaryCreate()
    val encodedPtr = NativeBridge.allocMainStore(rows)

    try {
      // 2. ENCODE (String -> Int)
      NativeBridge.dictionaryEncodeBatch(dictPtr, rawStrings, encodedPtr)

      // 3. JAVA STRING SORT (Baseline)
      // We clone to avoid sorting the original array used for integrity checks
      val javaStart = System.nanoTime()
      val javaCopy = rawStrings.clone()
      java.util.Arrays.sort(javaCopy.asInstanceOf[Array[Object]])
      val javaTime = (System.nanoTime() - javaStart) / 1e6

      // 4. NATIVE RADIX SORT (On Encoded Integers)
      val nativeStart = System.nanoTime()
      NativeBridge.radixSort(encodedPtr, rows)
      val nativeTime = (System.nanoTime() - nativeStart) / 1e6

      if (!warmup) {
        val speedup = javaTime / nativeTime
        println(s"| %-12s | %-12s | %15.2f | %15.2f | %9.2fx |".format(
          if(rows >= 1000000) s"${rows/1000000}M" else s"${rows/1000}K",
          if(distinctCount >= 1000) s"${distinctCount/1000}K" else s"$distinctCount",
          javaTime,
          nativeTime,
          speedup
        ))

        // 5. INTEGRITY CHECK (Spot check)
        val resultBuf = new Array[Int](1)
        NativeBridge.copyToScala(encodedPtr, resultBuf, 1)
        val decoded = NativeBridge.dictionaryDecode(dictPtr, resultBuf(0))
        decoded should not be null
        
        speedup should be > 2.0
      }
    } finally {
      NativeBridge.freeMainStore(encodedPtr)
      NativeBridge.dictionaryDestroy(dictPtr)
    }
  }
}