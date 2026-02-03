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
import java.io.{File, PrintWriter}
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.Executors

class ConcurrencySpec extends AnyFlatSpec with Matchers {

  // UPDATED: Scaling up to 100 Million
  // Note: 100M Ints = ~400MB RAM. Ensure your JVM has -Xmx2G or higher.
  val SIZES = Seq(10_000, 100_000, 1_000_000, 10_000_000) // 100M commented out for CI speed, uncomment to test locally
  // val SIZES = Seq(10_000, 100_000, 1_000_000, 10_000_000, 100_000_000) 
  
  val NUM_THREADS = 4
  
  // Custom Execution Context for generating load
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  "AwanDB EngineManager" should "benchmark Async Write Throughput across sizes" in {
    
    println("\n" + "=" * 80)
    println("| %-12s | %-20s | %-12s | %-15s |".format("ITEMS", "METRIC", "TIME (s)", "SPEED (Mops/s)"))
    println("-" * 80)

    for (totalItems <- SIZES) {
      // Run benchmark and force garbage collection after
      runBenchmark(totalItems)
      System.gc()
      Thread.sleep(1000) // Cool down
    }
  }

  def runBenchmark(totalItems: Int): Unit = {
    val testDir = s"data/concurrency_test_${totalItems}"
    cleanDir(testDir)
    new File(testDir).mkdirs()

    // 1. Setup Table
    val table = new AwanTable(s"bench_${totalItems}", totalItems, testDir)
    
    // [CRITICAL FIX] Must add a column, otherwise Insert crashes with "Expected 0 columns, got 1"
    table.addColumn("val") 
    
    val insertsPerThread = totalItems / NUM_THREADS
    val start = System.nanoTime()

    // 2. Launch Writers
    val futures = (1 to NUM_THREADS).map { id =>
      Future {
        for (i <- 1 to insertsPerThread) {
          // Use engineManager.submitInsert for non-blocking Async IO
          table.engineManager.submitInsert((id * 1000000) + i)
        }
      }
    }

    // 3. Measure API Submission Latency (Time to fill the Queue)
    // INCREASED TIMEOUT: 100M items might take time to allocate objects
    Await.result(Future.sequence(futures), 10.minutes)
    val submissionTime = (System.nanoTime() - start) / 1e9

    // 4. Measure Engine Processing (Time to Drain Queue & Write WAL)
    var rowsFound = 0
    // INCREASED TIMEOUT: Allow up to 10 minutes for 100M items processing
    val maxWait = System.nanoTime() + (600 * 1e9) 
    
    while (rowsFound < totalItems && System.nanoTime() < maxWait) {
      if (totalItems >= 10_000_000) Thread.sleep(100) else Thread.sleep(10)
      // Check count (using the new query API)
      rowsFound = table.query(-1) // Query all > -1
    }
    val processingTime = (System.nanoTime() - start) / 1e9

    // --- REPORTING ---
    val submissionRate = (totalItems / 1e6) / submissionTime
    val processingRate = (totalItems / 1e6) / processingTime
    
    println("| %-12d | %-20s | %12.4f | %15.2f |".format(totalItems, "API Submission", submissionTime, submissionRate))
    println("| %-12s | %-20s | %12.4f | %15.2f |".format("", "Engine (WAL)", processingTime, processingRate))
    println("-" * 80)

    rowsFound shouldBe totalItems
    table.close()
    cleanDir(testDir)
  }

  def cleanDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }
}