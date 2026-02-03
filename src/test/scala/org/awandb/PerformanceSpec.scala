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

import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge // <--- ADDED IMPORT
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileWriter, PrintWriter}
import scala.util.Random

class PerformanceSpec extends AnyFlatSpec with Matchers {

  // Configuration
  val SIZES = Seq(1_000, 10_000, 100_000, 1_000_000, 10_000_000) // Reduced 20M/50M for CI speed
  val CSV_FILE = "benchmark_scaling_log.csv"
  val INT_SIZE = 4.0 

  "AwanDB Performance" should "benchmark across different dataset sizes" in {
    new File(CSV_FILE).delete() 
    
    for (rows <- SIZES) {
      runBenchmark(rows)
      System.gc() 
      Thread.sleep(1000) 
    }
  }

  def runBenchmark(rows: Int): Unit = {
    // 1. Setup Unique Directory (Prevents WAL pollution)
    val testDir = s"data/bench_data_$rows" // Put in data folder
    cleanDir(testDir) 

    println(f"\n=== ${java.text.NumberFormat.getIntegerInstance.format(rows)} Rows Performance ===")
    println("| %-20s | %-10s | %-12s | %-10s |".format("METRIC", "TIME(ms)", "TP(Mops/s)", "BW(MB/s)"))
    println("-" * 65)

    // 2. Initialize Table with Unique Directory
    // FIX: Added name argument "bench_{rows}"
    val table = new AwanTable(s"bench_${rows}", rows, testDir) 
    table.addColumn("val") // Must add at least one column

    val random = new Random(42)
    
    val data = Array.fill(rows)(random.nextInt(100000))
    val readCount = Math.min(rows / 10, 100_000) 
    val randomIndices = Array.fill(readCount)(random.nextInt(rows))
    val threshold = 50000

    // --- 1. SEQUENTIAL WRITE ---
    val writeStart = System.nanoTime()
    var i = 0
    while (i < rows) {
      table.insert(data(i))
      i += 1
    }
    report(rows, "Seq Write", (System.nanoTime() - writeStart) / 1e9, rows)

    // --- 2. FLUSH ---
    val flushStart = System.nanoTime()
    table.flush()
    report(rows, "Flush", (System.nanoTime() - flushStart) / 1e9, rows)

    // --- 3. SEQ SCAN ---
    table.query(threshold) // Warmup
    val scanStart = System.nanoTime()
    table.query(threshold)
    report(rows, "Seq Scan (AVX)", (System.nanoTime() - scanStart) / 1e9, rows)

    // --- 4. RANDOM READ ---
    val outPtr = NativeBridge.allocMainStore(readCount)
    val indicesPtr = NativeBridge.allocMainStore(readCount)
    NativeBridge.loadData(indicesPtr, randomIndices)

    val randStart = System.nanoTime()
    
    // FIX: Retrieve the pointer from the BlockManager
    if (table.blockManager.getLoadedBlocks.nonEmpty) {
      val blockPtr = table.blockManager.getLoadedBlocks.head
      
      // Get pointer to Column 0 data
      val colDataPtr = NativeBridge.getColumnPtr(blockPtr, 0)
      
      // Perform Random Read
      NativeBridge.batchRead(colDataPtr, indicesPtr, readCount, outPtr)
    } else {
      println("[WARN] No blocks loaded for Random Read test!")
    }

    val randTime = (System.nanoTime() - randStart) / 1e9 // Seconds
    report(rows, s"Rand Read ($readCount)", randTime, readCount)

    // Cleanup
    NativeBridge.freeMainStore(outPtr)
    NativeBridge.freeMainStore(indicesPtr)
    table.close()
    
    // Clean disk after run
    cleanDir(testDir)
  }

  def report(rows: Int, metric: String, timeSec: Double, ops: Int): Unit = {
    // Avoid division by zero
    val timeSafe = if (timeSec == 0) 0.000001 else timeSec
    val throughput = (ops / 1_000_000.0) / timeSafe
    val bandwidth = (ops * INT_SIZE) / (1024 * 1024) / timeSafe
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