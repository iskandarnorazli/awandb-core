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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.io.File
import java.util.concurrent.{Executors, CountDownLatch, TimeUnit}
import org.awandb.core.engine.AwanTable

class ConcurrencyAndPerformanceSpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "perf_products"
  val dataDir = "data_perf_test"
  var table: AwanTable = _

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  override def beforeAll(): Unit = {
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()

    // Initialize with a large capacity
    table = new AwanTable(tableName, 500000, dataDir)
    table.addColumn("id")    
    table.addColumn("price") 
    table.addColumn("stock") 
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // 1. CONCURRENCY TEST (Thread-Safe Writes)
  // -------------------------------------------------------------------------

  test("1. Concurrency: 50 threads inserting simultaneously should not lose data") {
    val numThreads = 50
    val insertsPerThread = 200
    val totalExpected = numThreads * insertsPerThread

    val executor = Executors.newFixedThreadPool(16)
    val latch = new CountDownLatch(numThreads)

    // Fire 50 threads at the table
    for (t <- 0 until numThreads) {
      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            for (i <- 0 until insertsPerThread) {
              // Ensure globally unique IDs: 0 to 9999
              val id = (t * insertsPerThread) + i 
              table.insertRow(Array(id, 150, 20))
            }
          } finally {
            latch.countDown()
          }
        }
      })
    }

    // Wait for all threads to finish (max 10 seconds)
    assert(latch.await(10, TimeUnit.SECONDS), "Threads deadlocked or did not finish in time!")
    executor.shutdown()

    // Verify all rows made it to the RAM buffer safely
    // AwanTable.query(threshold) defaults to querying the first column (id)
    // Counting IDs > -1 should return everything
    val matched = table.query(-1)
    assert(matched == totalExpected, s"Race condition detected! Expected $totalExpected rows, got $matched")
  }

  // -------------------------------------------------------------------------
  // 2. PARALLEL SCAN TEST (MorselExec)
  // -------------------------------------------------------------------------

  test("2. Performance: MorselExec should correctly scan across multiple disk blocks") {
    // 1. Flush the 10,000 rows from the previous test to Disk (Block 0)
    table.flush()

    // 2. Insert another 20,000 rows (IDs 10,000 to 29,999)
    for (i <- 0 until 20000) {
       table.insertRow(Array(10000 + i, 250, 50))
    }
    
    // 3. Flush to Disk (Block 1)
    table.flush()

    // 4. Insert 5,000 rows into RAM (IDs 30,000 to 34,999)
    for (i <- 0 until 5000) {
       table.insertRow(Array(30000 + i, 999, 10))
    }

    // At this point, we have:
    // Block 0: 10,000 rows (IDs 0 - 9,999)
    // Block 1: 20,000 rows (IDs 10,000 - 29,999)
    // RAM    : 5,000 rows  (IDs 30,000 - 34,999)

    // Test a cross-boundary threshold query on the 'id' column
    // We want to count how many rows have an ID > 15,000.
    // Expected:
    // Block 0: 0 matches
    // Block 1: 14,999 matches (IDs 15,001 to 29,999)
    // RAM    : 5,000 matches
    // Total  : 19,999 matches

    val expectedCount = 19999
    val resultCount = table.query(15000)

    assert(resultCount == expectedCount, 
      s"Parallel scan failed! Expected $expectedCount but got $resultCount")
  }
}