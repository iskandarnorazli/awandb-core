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
import org.awandb.core.engine.AwanTable
import java.io.File
import scala.concurrent.{Future, Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class ConcurrencySQLSpec extends AnyFlatSpec with Matchers {

  // Reuse the clean helper
  def cleanSlate(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) deleteRecursively(dir)
  }
  
  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  // -------------------------------------------------------------------------
  // TEST 1: The "Flush Hazard"
  // (Writing to RAM while Flushing to Disk - The #1 cause of corruption)
  // -------------------------------------------------------------------------
  "AwanDB" should "survive heavy inserts while Flushing concurrently" in {
    val dir = "data_test_conc_flush"
    cleanSlate(dir)
    val table = new AwanTable("hazard_test", 100000, dir) // Large capacity
    table.addColumn("id")
    table.addColumn("val")

    val stopFlag = new java.util.concurrent.atomic.AtomicBoolean(false)

    // THREAD 1: The "Hammer" (Inserts as fast as possible)
    val writer = Future {
      var i = 0
      while (!stopFlag.get()) {
        table.insertRow(Array(i, i * 2))
        i += 1
      }
      i
    }

    // THREAD 2: The "Persistence Manager" (Flushes randomly)
    val flusher = Future {
      while (!stopFlag.get()) {
        Thread.sleep(10) // Flush every 10ms
        table.flush()
      }
      true
    }

    // Let them fight for 2 seconds
    Thread.sleep(2000)
    stopFlag.set(true)

    val totalInserted = Await.result(writer, 5.seconds)
    Await.result(flusher, 5.seconds)

    println(s"[FlushHazard] Inserted $totalInserted rows during flush storms.")

    // VERIFICATION:
    // If locking failed, we might have dropped rows or corrupted the block header.
    // We check if the last few inserted items are queryable.
    
    // Check random sample
    val sampleId = totalInserted - 50
    if (sampleId > 0) {
      val count = table.query("val", sampleId * 2)
      count should be (1)
    }

    table.close()
  }

  // -------------------------------------------------------------------------
  // TEST 2: The "Atomic Update" Race
  // (Reader must never see duplicates during an Update)
  // -------------------------------------------------------------------------
  it should "maintain consistency during concurrent UPDATE (Delete+Insert)" in {
    val dir = "data_test_conc_update"
    cleanSlate(dir)
    val table = new AwanTable("update_race", 10000, dir)
    table.addColumn("id")
    table.addColumn("status") // 1 = Active, 0 = Deleted

    // Seed ID 100
    table.insertRow(Array(100, 1))

    val startLatch = new java.util.concurrent.CountDownLatch(1)
    
    // THREAD 1: Updates ID 100 repeatedly (Delete -> Insert)
    val updater = Future {
      startLatch.await()
      for (i <- 1 to 1000) {
        table.update(100, Map("status" -> 1)) 
        // Note: Our current update() implementation in AwanTable performs a DELETE.
        // The user must INSERT the new row manually in Phase 6.
        // So we simulate the SQLHandler logic here:
        // 1. Delete is already done by table.update() inside the loop? 
        //    Wait, check AwanTable.update implementation. 
        //    Ah, currently table.update(id, map) calls delete(id).
        //    It DOES NOT insert the new row yet.
        
        table.insertRow(Array(100, 1)) // Re-insert immediately
      }
      true
    }

    // THREAD 2: Reads ID 100 constantly
    // Correctness Criteria: Should see 0 or 1. NEVER > 1.
    val watcher = Future {
      startLatch.await()
      var errors = 0
      for (_ <- 1 to 2000) {
        val count = table.query("id", 100)
        if (count > 1) {
           println(s"[FATAL] Race Condition! Found $count copies of ID 100")
           errors += 1
        }
      }
      errors
    }

    startLatch.countDown() // GO!
    
    Await.result(updater, 5.seconds)
    val errorCount = Await.result(watcher, 5.seconds)

    errorCount should be (0)
    
    // Final state check
    table.query("id", 100) should be (1) 

    table.close()
  }

  // -------------------------------------------------------------------------
  // TEST 3: The "Dictionary Integrity"
  // (Updating Strings shouldn't corrupt the dictionary)
  // -------------------------------------------------------------------------
  it should "handle concurrent String updates with Dictionary Compression" in {
    val dir = "data_test_conc_dict"
    cleanSlate(dir)
    val table = new AwanTable("dict_race", 10000, dir)
    table.addColumn("id")
    table.addColumn("city", isString = true, useDictionary = true)

    val cities = Array("KL", "SG", "BK", "JKT", "MNL")

    // Seed
    for (i <- 0 until 100) table.insertRow(Array(i, "KL"))

    // THREAD 1: Update cities randomly
    val updater = Future {
      for (_ <- 1 to 500) {
        val id = Random.nextInt(100)
        val city = cities(Random.nextInt(cities.length))
        table.delete(id)
        table.insertRow(Array(id, city))
      }
      true
    }
    
    // THREAD 2: Flush periodically to force dictionary encoding
    val flusher = Future {
      for (_ <- 1 to 10) {
        Thread.sleep(15)
        table.flush()
      }
      true
    }

    Await.result(updater, 5.seconds)
    Await.result(flusher, 5.seconds)

    // Verify Dictionary didn't crash
    val cnt = table.query("city", "KL")
    println(s"Final KL Count: $cnt") // Just ensure it doesn't throw exception

    table.close()
  }
}