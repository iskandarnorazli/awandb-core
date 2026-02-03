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
  val SIZES = Seq(1_000, 10_000, 100_000, 1_000_000, 10_000_000, 20_000_000, 50_000_000)
  val CSV_FILE = "benchmark_scaling_log.csv"
  val INT_SIZE = 4.0 

  "AwanDB Performance" should "benchmark across different dataset sizes" in {
    new File(CSV_FILE).delete() 
    
    // [JIT WARMUP] 
    // Run a tiny test first to force JVM to load classes and compile hot paths
    runBenchmark(10000, warmup = true) 
    
    for (rows <- SIZES) {
      runBenchmark(rows, warmup = false)
      System.gc() 
      Thread.sleep(1000) // Cool down
    }
  }

  def runBenchmark(rows: Int, warmup: Boolean): Unit = {
    val testDir = s"data/bench_data_$rows" 
    cleanDir(testDir) 

    if (!warmup) {
        println(f"\n=== ${java.text.NumberFormat.getIntegerInstance.format(rows)} Rows Performance ===")
        println("| %-20s | %-10s | %-12s | %-10s |".format("METRIC", "TIME(ms)", "TP(Mops/s)", "BW(MB/s)"))
        println("-" * 65)
    }

    val table = new AwanTable(s"bench_${rows}", rows, testDir) 
    table.addColumn("val")

    val random = new Random(42)
    val data = Array.fill(rows)(random.nextInt(100000))
    val readCount = Math.min(rows / 10, 100_000) 
    val randomIndices = Array.fill(readCount)(random.nextInt(rows))
    val threshold = 50000

    // --- 1. SEQUENTIAL WRITE ---
    val writeStart = System.nanoTime()
    table.insertBatch(data) // Use Batch Insert for stability
    if (!warmup) report(rows, "Seq Write", (System.nanoTime() - writeStart) / 1e9, rows)

    // --- 2. FLUSH ---
    val flushStart = System.nanoTime()
    table.flush()
    if (!warmup) report(rows, "Flush", (System.nanoTime() - flushStart) / 1e9, rows)

    // --- 3. SEQ SCAN (STABILIZED) ---
    // A. Warmup JIT & Cache (3 runs)
    for (i <- 1 to 3) {
       table.query(threshold)
    }
    
    // B. Measure Average (5 runs)
    var totalScanTime = 0.0
    val iterations = 5
    for (i <- 1 to iterations) {
       val start = System.nanoTime()
       table.query(threshold)
       totalScanTime += (System.nanoTime() - start)
    }
    
    if (!warmup) report(rows, "Seq Scan (AVX)", (totalScanTime / iterations) / 1e9, rows)

    // --- 4. RANDOM READ ---
    val outPtr = NativeBridge.allocMainStore(readCount)
    val indicesPtr = NativeBridge.allocMainStore(readCount)
    NativeBridge.loadData(indicesPtr, randomIndices)

    // Check if blocks loaded
    if (table.blockManager.getLoadedBlocks.nonEmpty) {
      val blockPtr = table.blockManager.getLoadedBlocks.head
      val colDataPtr = NativeBridge.getColumnPtr(blockPtr, 0)
      
      // Warmup Read
      NativeBridge.batchRead(colDataPtr, indicesPtr, readCount, outPtr)
      
      val randStart = System.nanoTime()
      NativeBridge.batchRead(colDataPtr, indicesPtr, readCount, outPtr)
      val randTime = (System.nanoTime() - randStart) / 1e9 
      
      if (!warmup) report(rows, s"Rand Read ($readCount)", randTime, readCount)
    } 

    NativeBridge.freeMainStore(outPtr)
    NativeBridge.freeMainStore(indicesPtr)
    table.close()
    
    cleanDir(testDir)
  }

  def report(rows: Int, metric: String, timeSec: Double, ops: Int): Unit = {
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