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

import org.awandb.core.engine.{AwanTable, MorselExec}
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random
import java.util.UUID

class DictionarySpec extends AnyFlatSpec with Matchers {

  // --- HELPER: Test Isolation ---
  def withTestDir(testCode: String => Any): Unit = {
    val uniqueId = UUID.randomUUID().toString.take(8)
    val dirPath = s"target/dict_test_$uniqueId"
    val dir = new File(dirPath)
    
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()
    
    try {
      testCode(dirPath)
    } finally {
      deleteRecursively(dir)
    }
  }

  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  // --- PART 1: STORAGE INTEGRATION ---

  "AwanDB Dictionary" should "seamlessly encode, persist, and query strings" in {
    withTestDir { dir =>
      val table = new AwanTable("dict_test", 1000, dir)
      table.addColumn("city", isString = true, useDictionary = true)
      
      // 1. Insert Data natively 
      table.insertRow(Array("Kuala Lumpur"))
      table.insertRow(Array("Penang"))
      table.insertRow(Array("Kuala Lumpur")) 
      
      // 2. Query RAM 
      table.query("city", "Kuala Lumpur") shouldBe 2
      
      // 3. Flush to Disk
      table.flush()
      
      // 4. Verify Sidecar File Exists
      val dictFile = new File(table.tableDir, "dict_test_city.dict")
      dictFile.exists() shouldBe true
      
      // 5. Query Disk 
      table.query("city", "Kuala Lumpur") shouldBe 2
      table.query("city", "Singapore") shouldBe 0 
      
      table.close()
    }
  }

  it should "demonstrate massive storage compression vs Raw Strings" in {
    withTestDir { dir =>
      val dictDir = new File(dir, "dict").getAbsolutePath
      val dictTable = new AwanTable("dict_bench", 10000, dictDir)
      dictTable.addColumn("data", isString = true, useDictionary = true)
      
      val prefixes = Array("Log_Entry_Error_", "User_Action_Click_", "Transaction_ID_", "Status_Pending_")
      val random = new Random(42)
      
      var logicalRawSizeBytes = 0L
      
      for (i <- 0 until 5000) {
        val value = prefixes(random.nextInt(prefixes.length)) + (i % 100)
        val padded = value + "_padding_" + "x" * 20 
        
        logicalRawSizeBytes += padded.getBytes("UTF-8").length
        dictTable.insertRow(Array(padded))
      }
      
      dictTable.flush()
      
      // MEASURE: Recursive check of actual physical bytes written to the table directory
      def dirSize(file: File): Long = {
        if (file.isDirectory) {
            val children = file.listFiles()
            if (children != null) children.map(dirSize).sum else 0L
        } else {
            file.length()
        }
      }

      val dictSize = dirSize(new File(dictTable.tableDir))
      
      println(f"\n[Compression Benchmark]")
      println(f"Logical Raw Size:  ${logicalRawSizeBytes / 1024} KB")
      println(f"Actual Dict Size:  ${dictSize / 1024} KB")
      
      if (dictSize > 0) {
         val ratio = logicalRawSizeBytes.toDouble / dictSize
         println(f"Compression:      $ratio%.2fx")
         logicalRawSizeBytes should be > dictSize
      } else {
         fail("Dictionary directory size was 0 bytes. Flush failed?")
      }
      
      dictTable.close()
    }
  }

  // --- PART 2: COMPUTE PERFORMANCE ---

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
    val distinctValues = (0 until distinctCount).map(i => s"val_prefix_${i}").toArray
    val rawStrings = Array.fill(rows)(distinctValues(Random.nextInt(distinctCount)))
    
    val dictPtr = NativeBridge.dictionaryCreate()
    val encodedPtr = NativeBridge.allocMainStore(rows)

    try {
      NativeBridge.dictionaryEncodeBatch(dictPtr, rawStrings, encodedPtr)

      val javaStart = System.nanoTime()
      val javaCopy = rawStrings.clone()
      java.util.Arrays.sort(javaCopy.asInstanceOf[Array[Object]])
      val javaTime = (System.nanoTime() - javaStart) / 1e6

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

        val resultBuf = new Array[Int](1)
        NativeBridge.copyToScala(encodedPtr, resultBuf, 1)
        val decoded = NativeBridge.dictionaryDecode(dictPtr, resultBuf(0))
        decoded should not be null
        
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