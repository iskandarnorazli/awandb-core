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

import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileWriter, PrintWriter}
import scala.util.Random

class PerformanceSpec extends AnyFlatSpec with Matchers {

  // Configuration
  val SIZES = Seq(1_000, 10_000, 100_000, 1_000_000, 5_000_000)
  val CSV_FILE = "benchmark_scaling_log.csv"
  val ROW_SIZE = 8.0 // 2 Integers = 8 Bytes

  "AwanDB Performance" should "benchmark Transactional CRUD operations" in {
    new File(CSV_FILE).delete() 
    
    println(">> Warming up JVM...")
    runBenchmark(10000, warmup = true) 
    
    for (rows <- SIZES) {
      runBenchmark(rows, warmup = false)
      System.gc() 
      Thread.sleep(1000) 
    }
  }

  def runBenchmark(rows: Int, warmup: Boolean): Unit = {
    val testDir = s"data/bench_data_$rows" 
    cleanDir(testDir) 

    if (!warmup) {
        println(f"\n=== ${java.text.NumberFormat.getIntegerInstance.format(rows)} Rows (Transactional) ===")
        println("| %-20s | %-10s | %-12s | %-10s |".format("METRIC", "TIME(ms)", "TP(Mops/s)", "BW(MB/s)"))
        println("-" * 65)
    }

    val table = new AwanTable(s"bench_${rows}", rows, testDir) 
    table.addColumn("id")  // Col 0
    table.addColumn("val") // Col 1

    val random = new Random(42)
    
    // Pre-generate data
    val ids = (0 until rows).toArray
    val payloads = Array.fill(rows)(random.nextInt(100000)) 
    
    val opCount = Math.min(rows / 10, 100_000)
    val randomIndices = Array.fill(opCount)(random.nextInt(rows))
    val threshold = 50000
    
    // --- 1. TRANSACTIONAL WRITE ---
    val writeStart = System.nanoTime()
    var i = 0
    while (i < rows) {
      table.insertRow(Array(ids(i), payloads(i)))
      i += 1
    }
    if (!warmup) report(rows, "Trans. Write", (System.nanoTime() - writeStart) / 1e9, rows)

    // --- 2. FLUSH ---
    val flushStart = System.nanoTime()
    table.flush()
    if (!warmup) report(rows, "Flush", (System.nanoTime() - flushStart) / 1e9, rows)

    // --- 3. SEQ SCAN (CLEAN) ---
    // Should hit the AVX Fast Path because blocks have no deletions yet.
    for (_ <- 1 to 3) table.query(threshold) // Warmup
    
    var totalScanTime = 0.0
    val iterations = 5
    for (_ <- 1 to iterations) {
       val start = System.nanoTime()
       table.query(threshold)
       totalScanTime += (System.nanoTime() - start)
    }
    if (!warmup) report(rows, "Seq Scan (Clean)", (totalScanTime / iterations) / 1e9, rows)

    // --- 4. RANDOM UPDATE (Delete + Insert) ---
    // This creates Bitmaps, marking blocks as "Dirty"
    val updateStart = System.nanoTime()
    var u = 0
    while (u < opCount) {
       val targetId = randomIndices(u)
       val newVal = payloads(targetId) + 1
       val changes = scala.collection.immutable.Map("val" -> newVal.asInstanceOf[Any])
       
       if (table.update(targetId, changes)) {
           table.insertRow(Array(targetId, newVal))
       }
       u += 1
    }
    val updateDur = (System.nanoTime() - updateStart) / 1e9
    if (!warmup) report(rows, s"Rand Update ($opCount)", updateDur, opCount)

    // --- 5. SEQ SCAN (DIRTY) ---
    // Should hit the Scala Slow Path because blocks now have deletions.
    var dirtyScanTime = 0.0
    for (_ <- 1 to iterations) {
       val start = System.nanoTime()
       table.query(threshold)
       dirtyScanTime += (System.nanoTime() - start)
    }
    if (!warmup) report(rows, "Seq Scan (Dirty)", (dirtyScanTime / iterations) / 1e9, rows)

    // --- 6. RANDOM READ ---
    if (table.blockManager.getLoadedBlocks.nonEmpty) {
      val outPtr = NativeBridge.allocMainStore(opCount)
      val indicesPtr = NativeBridge.allocMainStore(opCount)
      NativeBridge.loadData(indicesPtr, randomIndices)

      val blockPtr = table.blockManager.getLoadedBlocks.head
      val colDataPtr = NativeBridge.getColumnPtr(blockPtr, 1) 
      
      NativeBridge.batchRead(colDataPtr, indicesPtr, opCount, outPtr) // Warmup
      
      val randStart = System.nanoTime()
      NativeBridge.batchRead(colDataPtr, indicesPtr, opCount, outPtr)
      val randTime = (System.nanoTime() - randStart) / 1e9 
      
      if (!warmup) report(rows, s"Rand Read ($opCount)", randTime, opCount)
      
      NativeBridge.freeMainStore(outPtr)
      NativeBridge.freeMainStore(indicesPtr)
    } 

    table.close()
    cleanDir(testDir)
  }

  def report(rows: Int, metric: String, timeSec: Double, ops: Int): Unit = {
    val timeSafe = if (timeSec == 0) 0.000001 else timeSec
    val throughput = (ops / 1_000_000.0) / timeSafe
    val bandwidth = (ops * ROW_SIZE) / (1024 * 1024) / timeSafe
    println(f"| $metric%-20s | ${timeSafe*1000}%10.3f | $throughput%12.2f | $bandwidth%10.2f |")
    logToCsv(rows, metric, timeSafe * 1000, throughput, bandwidth)
  }

  def logToCsv(rows: Int, metric: String, ms: Double, mops: Double, mbps: Double): Unit = {
    val file = new File(CSV_FILE)
    val writer = new PrintWriter(new FileWriter(file, true))
    if (file.length() == 0) writer.println("Rows,Metric,Time_MS,Mops_Sec,MB_Sec")
    writer.println(s"$rows,$metric,$ms,$mops,$mbps")
    writer.close()
  }

  def cleanDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }
}