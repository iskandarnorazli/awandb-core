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

package org.awandb.core.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.Random

class ConcurrentWorkloadSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = "data/bench_concurrent"
  
  // CONFIGURATION
  val WRITER_THREADS = 2
  val READER_THREADS = 4
  val DURATION_SECONDS = 5
  val BATCH_SIZE = 1000 // For the "Fused" test

  def clearData(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  def runConcurrencyTest(testName: String, useBatching: Boolean): Unit = {
    println(s"\n--- $testName (Writers: $WRITER_THREADS, Readers: $READER_THREADS) ---")
    clearData()
    
    val table = new AwanTable("concurrent_table", 10000000, TEST_DIR)
    table.addColumn("col1")
    
    // Counters
    val totalInserts = new AtomicLong(0)
    val totalQueries = new AtomicLong(0)
    val running = new AtomicBoolean(true)
    
    val pool = Executors.newFixedThreadPool(WRITER_THREADS + READER_THREADS)

    // -------------------------------------------------------
    // 1. START WRITERS (The "Lock Hoggers")
    // -------------------------------------------------------
    for (i <- 1 to WRITER_THREADS) {
      pool.submit(new Runnable {
        override def run(): Unit = {
          val batchData = Array.fill(BATCH_SIZE)(Random.nextInt())
          while (running.get()) {
            if (useBatching) {
              // OPTIMIZED: 1 Lock per 1000 rows
              table.insertBatch(batchData)
              totalInserts.addAndGet(BATCH_SIZE)
            } else {
              // NAIVE: 1000 Locks per 1000 rows
              // This simulates massive contention ("Death by 1000 cuts")
              var j = 0
              while (j < BATCH_SIZE) {
                table.insert(batchData(j))
                j += 1
              }
              totalInserts.addAndGet(BATCH_SIZE)
            }
            // Small sleep to prevent complete system freeze in naive mode
            // (Simulates network latency)
            Thread.sleep(1) 
          }
        }
      })
    }

    // -------------------------------------------------------
    // 2. START READERS (The "Victims")
    // -------------------------------------------------------
    for (i <- 1 to READER_THREADS) {
      pool.submit(new Runnable {
        override def run(): Unit = {
          while (running.get()) {
            // Readers try to get the ReadLock. 
            // If Writers hold the WriteLock too long, Readers starve.
            table.query(Random.nextInt())
            totalQueries.incrementAndGet()
          }
        }
      })
    }

    // -------------------------------------------------------
    // 3. MEASURE
    // -------------------------------------------------------
    println(s"Running for $DURATION_SECONDS seconds...")
    Thread.sleep(DURATION_SECONDS * 1000)
    
    running.set(false)
    pool.shutdown()
    pool.awaitTermination(2, TimeUnit.SECONDS)
    table.close()

    // -------------------------------------------------------
    // 4. REPORT
    // -------------------------------------------------------
    val readsPerSec = totalQueries.get() / DURATION_SECONDS
    val writesPerSec = totalInserts.get() / DURATION_SECONDS
    
    println(f"""
    |=========================================================
    | RESULTS: $testName
    |---------------------------------------------------------
    | Total Writes  : ${totalInserts.get()}
    | Total Reads   : ${totalQueries.get()}
    |
    | Write Speed   : $writesPerSec%,d rows/sec
    | Read Speed    : $readsPerSec%,d queries/sec
    |=========================================================
    """.stripMargin)
  }

  "AwanDB" should "demonstrate the impact of Write Lock Contention" in {
    // TEST A: The "Bad" Way (Row-by-Row)
    // Writers constantly acquire/release locks, causing high CPU context switching overhead.
    // Readers struggle to find a gap to enter.
    runConcurrencyTest("Standard Concurrent Write (Row-by-Row)", useBatching = false)

    // TEST B: The "Optimized" Way (Batch / Fusion)
    // Writers hold the lock ONCE for a microsecond to dump data.
    // Readers have huge windows of time to execute concurrently.
    runConcurrencyTest("Fused Concurrent Write (Batching)", useBatching = true)
  }
}