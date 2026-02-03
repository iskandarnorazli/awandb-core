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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random

class SharedScanPerformanceSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = "data/shared_scan_perf"
  val ROW_COUNT = 500_000  // Bumping to 500k now that we fixed the bug
  val NUM_QUERIES = 100

  def cleanDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) { dir.listFiles().foreach(_.delete()); dir.delete() }
  }

  "AwanDB Query Engine" should "benchmark Query Fusion (Shared Scan) vs Standard Scan" in {
    cleanDir(TEST_DIR)
    new File(TEST_DIR).mkdirs()

    println(s"\n--- QUERY FUSION BENCHMARK ($ROW_COUNT rows, $NUM_QUERIES queries) ---")
    
    // FIX: Add name "shared_perf" to constructor
    val table = new AwanTable("shared_perf", ROW_COUNT * 2, TEST_DIR)
    table.addColumn("val") // Must add a column before inserting

    val dataStart = System.nanoTime()
    
    // 1. Data Generation
    println("Submitting Insert Commands...")
    for (i <- 0 until ROW_COUNT) {
      // FIX: Use engineManager.submitInsert for async operations
      // Ensure values are >= 1 so query(0) finds them all for count check
      table.engineManager.submitInsert(Random.nextInt(10000) + 1)
    }
    
    // 2. Wait for Processing
    println("Waiting for Engine...")
    var currentCount = 0
    var stuckCounter = 0
    var lastCount = 0

    while (currentCount < ROW_COUNT) {
      Thread.sleep(200) 
      currentCount = table.query(0)
      
      // Progress Bar
      if (currentCount % 50000 == 0 || currentCount == ROW_COUNT) {
        print(f"\rRows: $currentCount / $ROW_COUNT  ")
      }
      
      // Timeout Guard
      if (currentCount == lastCount && currentCount < ROW_COUNT) {
        stuckCounter += 1
        if (stuckCounter > 50) { // 10 seconds
           println("\n[ERROR] Engine stuck.")
           table.close()
           fail(s"Engine stuck at $currentCount / $ROW_COUNT")
        }
      } else {
        stuckCounter = 0
      }
      lastCount = currentCount
    }
    println(f"\nData Ready. Time: ${(System.nanoTime() - dataStart)/1e9}%.2fs")

    val thresholds = Array.fill(NUM_QUERIES)(Random.nextInt(10000))

    // ---------------------------------------------------------
    // METHOD A: STANDARD LOOP
    // ---------------------------------------------------------
    System.gc()
    Thread.sleep(500)
    val startNaive = System.nanoTime()
    var hitsNaive = 0L
    for (t <- thresholds) hitsNaive += table.query(t) 
    val durNaive = (System.nanoTime() - startNaive) / 1e9
    
    // ---------------------------------------------------------
    // METHOD B: SHARED SCAN
    // ---------------------------------------------------------
    System.gc()
    Thread.sleep(500)
    
    // Warmup (Optional, to load code into JIT)
    table.queryShared(thresholds.take(10)) 
    
    val startShared = System.nanoTime()
    val results = table.queryShared(thresholds)
    val hitsShared = results.map(_.toLong).sum
    val durShared = (System.nanoTime() - startShared) / 1e9

    // ---------------------------------------------------------
    // REPORT
    // ---------------------------------------------------------
    val speedup = if (durShared > 0) durNaive / durShared else 0.0
    
    println("\n" + "=" * 65)
    println("| %-20s | %-15s | %-15s |".format("METHOD", "TIME (s)", "THROUGHPUT"))
    println("-" * 65)
    println("| %-20s | %10.4f s    | %10.2f Q/s  |".format("Standard Loop", durNaive, NUM_QUERIES / durNaive))
    println("| %-20s | %10.4f s    | %10.2f Q/s  |".format("Shared Scan (Fused)", durShared, NUM_QUERIES / durShared))
    println("-" * 65)
    println(f"🚀 SPEEDUP FACTOR: ${speedup}%.2fx faster")
    println("=" * 65)

    hitsNaive shouldBe hitsShared
    table.close()
    cleanDir(TEST_DIR)
  }
}