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
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.Random
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.UUID

// [CRITICAL] Import the engine from the core package
import org.awandb.core.engine.AwanTable

class ConcurrentWorkloadSpec extends AnyFlatSpec with Matchers {

  // CONFIGURATION
  val WRITER_THREADS = 4
  val READER_THREADS = 4
  val TOTAL_ITEMS = 500_000 // Total items to insert
  val FLUSH_INTERVAL_MS = 200 // Force disk flushes frequently

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

  // [ISOLATION] Helper to create unique directories per test run
  def withTestDir(testCode: String => Any): Unit = {
    val uniqueId = UUID.randomUUID().toString.take(8)
    val dirPath = s"target/bench_concurrent_chaos_$uniqueId"
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

  "AwanDB" should "maintain data integrity under heavy R/W + Flush contention" in {
    withTestDir { testDir =>
      println(s"\n--- STARTING CHAOS TEST: $TOTAL_ITEMS rows ---")

      val table = new AwanTable("chaos_table", TOTAL_ITEMS, testDir)
      table.addColumn("id", isString = false)
      table.addColumn("data", isString = true) // Stresses Dictionary Locking

      val running = new AtomicBoolean(true)
      val totalInserts = new AtomicLong(0)
      
      // 1. CHAOS WRITERS
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
              // Equality check
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
      
      // Final flush to ensure all data hits disk for verification
      table.flush()

      println(s"\n--- WRITES COMPLETE. VERIFYING INTEGRITY... ---")

      // 5. [CRITICAL] DATA INTEGRITY CHECKS
      
      // Check A: Total Count
      // [FIX] Use query(-1) for Range Scan (> -1). 
      // query("id", -1) checks Equality (== -1) and would return 0.
      val finalCount = table.query(-1)
      println(f"   Expected: $TOTAL_ITEMS%,d")
      println(f"   Actual:   $finalCount%,d")
      finalCount shouldBe TOTAL_ITEMS

      // Check B: Specific Data Retrieval (Zone Map Verification)
      // We pick a random ID that definitely exists.
      val sampleId = (TOTAL_ITEMS / 2)
      
      // Check strict equality to ensure index precision
      val foundInt = table.query("id", sampleId) 
      foundInt shouldBe 1

      // Check C: String Dictionary Consistency
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
}