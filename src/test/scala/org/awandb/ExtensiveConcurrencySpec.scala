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
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicLong
import java.util.UUID

class ExtensiveConcurrencySpec extends AnyFlatSpec with Matchers {

  // Use a moderate size to allow room for contention overhead
  val TOTAL_ITEMS = 1_000_000 
  val WRITER_THREADS = 4
  val READER_THREADS = 2 
  
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  // [ISOLATION] Unique directory per test to prevent "Zombie Data"
  def withTestDir(testCode: String => Any): Unit = {
    val uniqueId = UUID.randomUUID().toString.take(8)
    val dirPath = s"target/stress_test_$uniqueId"
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

  "AwanDB EngineManager" should "handle Read-While-Write contention without data corruption" in {
    withTestDir { testDir =>
      val table = new AwanTable("stress_table", TOTAL_ITEMS, testDir)
      table.addColumn("id", isString = false)
      table.addColumn("data", isString = true) // Test Dictionary Locking

      val expectedSum = new AtomicLong(0)
      val start = System.nanoTime()

      println(s"[START] Launching $WRITER_THREADS Writers vs $READER_THREADS Readers...")

      // 1. LAUNCH WRITERS
      val writerFutures = (1 to WRITER_THREADS).map { id =>
        Future {
          val itemsPerThread = TOTAL_ITEMS / WRITER_THREADS
          var i = 0
          while (i < itemsPerThread) {
            // Generate unique, positive IDs
            // Thread 1: 0..249k, Thread 2: 250k..499k, etc.
            val value = ((id - 1) * itemsPerThread) + i
            
            // Mixed Type Insert: Int + String
            table.insertRow(Array[Any](value, s"val_$value"))
            
            expectedSum.addAndGet(value)
            i += 1
          }
        }
      }

      // 2. LAUNCH READERS (Chaos Monkeys)
      val readerFutures = (1 to READER_THREADS).map { id =>
        Future {
          var reads = 0
          while (!writerFutures.forall(_.isCompleted)) {
            // Query random IDs to trigger AVX Scans on shifting memory
            val randomId = scala.util.Random.nextInt(TOTAL_ITEMS)
            try {
              // Equality Query (id == randomId)
              table.query("id", randomId) 
              reads += 1
              if (reads % 1000 == 0) Thread.sleep(1) 
            } catch {
              case e: Exception => println(s"[READER FAIL] $e")
            }
          }
          println(s"   -> Reader $id completed $reads queries during write phase.")
        }
      }

      // 3. Wait for Writers
      Await.result(Future.sequence(writerFutures), 5.minutes)
      println("[INFO] Writers finished. Waiting for Engine to drain queue...")

      // 4. Wait for Consistency (Drain Queue / Flush)
      var rowsFound = 0
      val maxWait = System.nanoTime() + (60 * 1e9) // 60s timeout
      
      // We iterate until the count matches, or timeout.
      while (rowsFound < TOTAL_ITEMS && System.nanoTime() < maxWait) {
        Thread.sleep(100)
        
        // [CRITICAL FIX] Use Range Query API: query(-1) -> "id > -1"
        // Previous code used query("id", -1) which checked equality (id == -1) and returned 0.
        rowsFound = table.query(-1)
      }

      val duration = (System.nanoTime() - start) / 1e9
      println(s"[DONE] Processed $TOTAL_ITEMS rows in ${duration}s")

      // 5. INTEGRITY CHECKS
      
      // Check A: Total Row Count
      rowsFound shouldBe TOTAL_ITEMS
      
      // Check B: Sample Lookup
      val sampleID = 12345
      table.query("id", sampleID) shouldBe 1

      // Check C: String Consistency (Dictionary Lookups)
      val stringQueryCount = table.query("data", s"val_$sampleID")
      stringQueryCount shouldBe 1

      table.close()
    }
  }
}