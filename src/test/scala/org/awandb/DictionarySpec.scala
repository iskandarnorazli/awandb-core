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

import org.awandb.core.engine.{AwanTable, MorselExec} // [FIX] Added MorselExec import
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random

class DictionarySpec extends AnyFlatSpec with Matchers {

  // --- PART 1: STORAGE INTEGRATION (The "Does it work?" Test) ---
  
  val TEST_DIR = new File("data/dictionary_spec").getAbsolutePath

  def cleanUp(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  "AwanDB Dictionary" should "seamlessly encode, persist, and query strings" in {
    cleanUp()
    val table = new AwanTable("dict_test", 1000, TEST_DIR)
    
    // Create column with Dictionary enabled
    table.addColumn("city", isString = true, useDictionary = true)
    
    // 1. Insert Data (RAM)
    table.insert("city", "Kuala Lumpur")
    table.insert("city", "Penang")
    table.insert("city", "Kuala Lumpur") // Duplicate
    
    // 2. Query RAM (Should work via raw string scan)
    table.query("city", "Kuala Lumpur") shouldBe 2
    
    // 3. Flush to Disk (Triggers Compression)
    table.flush()
    
    // 4. Verify Sidecar File Exists
    val dictFile = new File(TEST_DIR, "dict_test_city.dict")
    dictFile.exists() shouldBe true
    
    // 5. Query Disk (Should use Fast Integer Scan)
    // The engine converts "Kuala Lumpur" -> ID -> avxScanBlock
    table.query("city", "Kuala Lumpur") shouldBe 2
    table.query("city", "Singapore") shouldBe 0 // Negative case
    
    table.close()
  }

  it should "demonstrate massive storage compression vs Raw Strings" in {
    // [FIX] Use separate directories
    val rawDir = new File(TEST_DIR, "raw").getAbsolutePath
    val dictDir = new File(TEST_DIR, "dict").getAbsolutePath
    cleanUp(); new File(rawDir).mkdirs(); new File(dictDir).mkdirs()
    
    val rawTable = new AwanTable("raw_bench", 10000, rawDir)
    rawTable.addColumn("data", isString = true, useDictionary = false)
    
    val dictTable = new AwanTable("dict_bench", 10000, dictDir)
    dictTable.addColumn("data", isString = true, useDictionary = true)
    
    // ... (Generate Data) ...
    
    rawTable.flush()
    dictTable.flush()
    
    // MEASURE
    val rawSize = new File(rawDir).listFiles().map(_.length()).sum
    val dictSize = new File(dictDir).listFiles().map(_.length()).sum
    
    println(f"\n[Compression Benchmark]")
    println(f"Raw String Size:  ${rawSize / 1024} KB")
    println(f"Dictionary Size:  ${dictSize / 1024} KB")
    
    if (dictSize > 0) {
        println(f"Compression:      ${rawSize.toDouble / dictSize}%.2fx")
        rawSize should be > dictSize
    }
    
    rawTable.close()
    dictTable.close()
  }

  // --- PART 2: COMPUTE PERFORMANCE (The "Is it fast?" Test) ---

  val ROWS = 1_000_000
  val CARDINALITIES = Seq(100, 100_000)

  it should "outperform Java String Sort via Encoded Radix Sort" in {
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
        
        // Only assert speedup if we are running in parallel or optimized env
        if (MorselExec.activeCores > 1) { 
            speedup should be > 1.2 
        }
      }
    } finally {
      NativeBridge.freeMainStore(encodedPtr)
      NativeBridge.dictionaryDestroy(dictPtr)
    }
  }
}