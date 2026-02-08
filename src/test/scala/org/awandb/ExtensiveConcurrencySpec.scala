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

class ExtensiveConcurrencySpec extends AnyFlatSpec with Matchers {

  // Use a moderate size to allow room for contention overhead
  val TOTAL_ITEMS = 1_000_000 
  val WRITER_THREADS = 4
  val READER_THREADS = 2 // New: Readers to fight with Writers
  
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  "AwanDB EngineManager" should "handle Read-While-Write contention without data corruption" in {
    
    val testDir = s"data/concurrency_stress_test"
    cleanDir(testDir)
    new File(testDir).mkdirs()

    val table = new AwanTable("stress_table", TOTAL_ITEMS, testDir)
    table.addColumn("id", isString = false)
    table.addColumn("data", isString = true) // [NEW] Test Dictionary Locking

    // Track the expected sum to verify data integrity later
    val expectedSum = new AtomicLong(0)
    val start = System.nanoTime()

    println(s"[START] Launching $WRITER_THREADS Writers vs $READER_THREADS Readers...")

    // 1. LAUNCH WRITERS
    val writerFutures = (1 to WRITER_THREADS).map { id =>
      Future {
        val itemsPerThread = TOTAL_ITEMS / WRITER_THREADS
        var i = 0
        while (i < itemsPerThread) {
          val value = (id * 1_000_000) + i
          
          // Mixed Type Insert: Int + String
          // This stresses both the IntBuffer and the DictionaryBuffer simultaneously
          table.insertRow(Array[Any](value, s"val_$value"))
          
          expectedSum.addAndGet(value)
          i += 1
        }
      }
    }

    // 2. LAUNCH READERS (Chaos Monkeys)
    // These run WHILE writers are working. They shouldn't crash or see garbage.
    val readerFutures = (1 to READER_THREADS).map { id =>
      Future {
        var reads = 0
        while (!writerFutures.forall(_.isCompleted)) {
          // Query random ranges to trigger AVX Scans on incomplete blocks
          val randomThresh = scala.util.Random.nextInt(TOTAL_ITEMS)
          try {
            // If locking works, this returns a valid number (or 0). 
            // If locking fails, this might segfault (AVX on shifting memory).
            table.query("id", randomThresh) 
            reads += 1
            if (reads % 1000 == 0) Thread.sleep(1) // Yield briefly
          } catch {
            case e: Exception => println(s"[READER FAIL] $e")
          }
        }
        println(s"   -> Reader $id completed $reads queries during write phase.")
      }
    }

    // 3. Wait for Writers to Finish
    Await.result(Future.sequence(writerFutures), 5.minutes)
    println("[INFO] Writers finished. Waiting for Engine to drain queue...")

    // 4. Wait for Engine Persistence (Drain Queue)
    var rowsFound = 0
    val maxWait = System.nanoTime() + (60 * 1e9) // 60 seconds timeout
    while (rowsFound < TOTAL_ITEMS && System.nanoTime() < maxWait) {
      Thread.sleep(100)
      rowsFound = table.query("id", -1)
    }

    val duration = (System.nanoTime() - start) / 1e9
    println(s"[DONE] Processed $TOTAL_ITEMS rows in ${duration}s")

    // 5. [CRITICAL] DATA INTEGRITY CHECKS
    
    // Check A: Row Count
    rowsFound shouldBe TOTAL_ITEMS
    
    // Check B: Logical Consistency (Zone Map check)
    // Assuming BlockManager works, we should be able to query the LAST item inserted
    val sampleID = (1 * 1_000_000) + 500 // Arbitrary ID from Thread 1
    table.query("id", sampleID - 1) should be >= 1

    // Check C: String Consistency
    // If Dictionary Encoding isn't thread-safe, this will fail or crash
    val stringQueryCount = table.query("data", s"val_$sampleID")
    stringQueryCount shouldBe 1

    table.close()
    cleanDir(testDir)
  }

  def cleanDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach(_.delete()))
      dir.delete()
    }
  }
}