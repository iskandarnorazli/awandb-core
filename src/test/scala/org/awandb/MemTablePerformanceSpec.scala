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
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileWriter, PrintWriter}
import scala.util.Random

class MemTablePerformanceSpec extends AnyFlatSpec with Matchers {

  // Configuration
  val SIZES = Seq(1_000, 10_000, 100_000, 1_000_000, 10_000_000, 20_000_000, 50_000_000)
  val CSV_FILE = "benchmark_memtable_log.csv"
  val INT_SIZE = 4.0 
  val CHUNK_SIZE = 1_000_000 

  "AwanDB MemTable Engine" should "benchmark parallel scanning across dataset sizes" in {
    new File(CSV_FILE).delete() 
    println("[Warmup] JIT compiling core paths...")
    runBenchmark(100_000, warmup = true) 
    
    for (rows <- SIZES) {
      runBenchmark(rows, warmup = false)
      System.gc() 
      Thread.sleep(500) 
    }
  }

  def runBenchmark(rows: Int, warmup: Boolean): Unit = {
    val testDir = s"data/bench_memtable_$rows" 
    cleanDir(testDir) 

    if (!warmup) {
        println(f"\n=== ${java.text.NumberFormat.getIntegerInstance.format(rows)} Rows Performance ===")
        println("| %-20s | %-10s | %-12s | %-10s |".format("METRIC", "TIME(ms)", "TP(Mops/s)", "BW(MB/s)"))
        println("-" * 65)
    }

    // 1. Initialize Table
    val table = new AwanTable(s"bench_${rows}", rows, testDir) 
    table.addColumn("val")

    val random = new Random(42)
    val data = Array.fill(rows)(random.nextInt(100000))
    val threshold = 50000

    // --- 2. DIRECT NATIVE INGESTION ---
    val writeStart = System.nanoTime()
    
    if (rows > CHUNK_SIZE) {
        data.grouped(CHUNK_SIZE).foreach { chunk =>
            table.blockManager.createAndPersistBlock(List(chunk))
        }
    } else {
        table.blockManager.createAndPersistBlock(List(data))
    }
    
    val totalWriteTime = (System.nanoTime() - writeStart) / 1e9
    
    if (!warmup) report(rows, "Seq Write (Direct)", totalWriteTime, rows)

    // Verify Block Count
    val blockCount = table.blockManager.getLoadedBlocks.size
    if (!warmup && rows > CHUNK_SIZE) {
        println(f"| Blocks Created       |          - | $blockCount%12d |          - |")
    }

    // --- 3. PARALLEL SEQ SCAN (MorselExec) ---
    // A. Warmup JIT & Cache
    for (i <- 1 to 3) table.query(threshold)
    
    // B. Measure Average
    var totalScanTime = 0.0
    val iterations = 5
    for (i <- 1 to iterations) {
       val start = System.nanoTime()
       table.query(threshold) 
       totalScanTime += (System.nanoTime() - start)
    }
    
    if (!warmup) report(rows, "Seq Scan (Morsel)", (totalScanTime / iterations) / 1e9, rows)

    // --- 4. RANDOM READ (IO Latency Check) ---
    // [FIX] We generate indices relative to the SINGLE BLOCK we are testing.
    // If rows=10M but block=1M, we must only ask for indices 0..1M.
    val actualBlockSize = if (rows > CHUNK_SIZE) CHUNK_SIZE else rows
    val readCount = Math.min(actualBlockSize / 10, 100_000) 
    val randomIndices = Array.fill(readCount)(random.nextInt(actualBlockSize))

    if (table.blockManager.getLoadedBlocks.nonEmpty) {
      val outPtr = NativeBridge.allocMainStore(readCount)
      val indicesPtr = NativeBridge.allocMainStore(readCount)
      NativeBridge.loadData(indicesPtr, randomIndices)

      // We test read speed against the FIRST block only
      val blockPtr = table.blockManager.getLoadedBlocks.head
      val colDataPtr = NativeBridge.getColumnPtr(blockPtr, 0)
      
      // Warmup
      NativeBridge.batchRead(colDataPtr, indicesPtr, readCount, outPtr)
      
      val randStart = System.nanoTime()
      NativeBridge.batchRead(colDataPtr, indicesPtr, readCount, outPtr)
      val randTime = (System.nanoTime() - randStart) / 1e9 
      
      if (!warmup) report(rows, s"Rand Read ($readCount)", randTime, readCount)
      
      NativeBridge.freeMainStore(outPtr)
      NativeBridge.freeMainStore(indicesPtr)
    } 

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