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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.util.concurrent.{Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.Random
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._

// [CRITICAL] Import the engine from the core package
import org.awandb.core.engine.AwanTable

class ConcurrentWorkloadSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = "data/bench_concurrent_chaos"
  
  // CONFIGURATION
  val WRITER_THREADS = 4
  val READER_THREADS = 4
  val TOTAL_ITEMS = 500_000 // Total items to insert
  val FLUSH_INTERVAL_MS = 200 // Force disk flushes frequently

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

  def clearData(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach(_.delete()))
      dir.delete()
    }
    new File(TEST_DIR).mkdirs()
  }

  "AwanDB" should "maintain data integrity under heavy R/W + Flush contention" in {
    clearData()
    println(s"\n--- STARTING CHAOS TEST: $TOTAL_ITEMS rows ---")

    val table = new AwanTable("chaos_table", TOTAL_ITEMS, TEST_DIR)
    table.addColumn("id", isString = false)
    table.addColumn("data", isString = true) // [NEW] Stresses Dictionary Locking

    val running = new AtomicBoolean(true)
    val totalInserts = new AtomicLong(0)
    
    // 1. CHAOS WRITERS
    // Each thread takes a unique range to verify exact row presence later
    val writers = (0 until WRITER_THREADS).map { threadId =>
      Future {
        val itemsPerThread = TOTAL_ITEMS / WRITER_THREADS
        var i = 0
        while (i < itemsPerThread) {
          val uniqueId = (threadId * itemsPerThread) + i
          
          // Mixed Type Insert: Stresses both IntBuffer and String Pool
          table.insertRow(Array[Any](uniqueId, s"val_$uniqueId"))
          
          i += 1
          totalInserts.incrementAndGet()
        }
        println(s"   -> Writer $threadId finished.")
      }
    }

    // 2. CHAOS READERS (The Victims)
    val readers = (0 until READER_THREADS).map { id =>
      Future {
        while (running.get()) {
          // Query random ranges. If locking fails, this might segfault Native code.
          val rnd = Random.nextInt(TOTAL_ITEMS)
          try {
            table.query("id", rnd)
          } catch {
            case e: Exception => 
              println(s"   [READER ERROR] ${e.getMessage}")
              throw e // Fail the test
          }
          Thread.sleep(1) // Yield to allow writers in
        }
      }
    }

    // 3. CHAOS FLUSHER (The Destabilizer)
    // Forces data from RAM -> Disk while others are reading/writing
    val flusher = Future {
      while (running.get()) {
        Thread.sleep(FLUSH_INTERVAL_MS)
        // This is the most dangerous moment: Moving pointers while threads are active
        table.flush()
      }
    }

    // 4. WAIT FOR WRITERS
    Await.result(Future.sequence(writers), 2.minutes)
    running.set(false) // Stop readers and flusher
    
    Await.result(Future.sequence(readers), 10.seconds)
    Await.result(flusher, 10.seconds)

    println(s"\n--- WRITES COMPLETE. VERIFYING INTEGRITY... ---")

    // 5. [CRITICAL] DATA INTEGRITY CHECKS
    
    // Check A: Total Count
    // This proves we didn't "drop" rows during race conditions
    // It also PROVES that MorselExec is partitioning correctly (Fixed 16x Bug)
    val finalCount = table.query("id", -1)
    println(f"   Expected: $TOTAL_ITEMS%,d")
    println(f"   Actual:   $finalCount%,d")
    finalCount shouldBe TOTAL_ITEMS

    // Check B: Specific Data Retrieval (Zone Map Verification)
    // We pick a random ID that definitely exists and try to find it.
    // If the ZoneMap is corrupted, this query will return 0.
    val sampleId = (TOTAL_ITEMS / 2)
    val foundInt = table.query("id", sampleId - 1) // Query > sampleId-1
    foundInt should be >= 1

    // Check C: String Dictionary Consistency
    // If the Dictionary wasn't locked correctly, this string might be garbage or point to wrong ID
    val sampleStr = s"val_$sampleId"
    val foundStr = table.query("data", sampleStr)
    if (foundStr != 1) {
      println(s"   [FAILURE] Could not find string '$sampleStr'. Dictionary might be corrupted.")
    }
    foundStr shouldBe 1

    table.close()
    println("--- CHAOS TEST PASSED ---")
  }
}