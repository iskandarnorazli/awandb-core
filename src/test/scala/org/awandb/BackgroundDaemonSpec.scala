/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.engine

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class BackgroundDaemonSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var table: AwanTable = _
  val testDir = "target/data_daemon"

  override def beforeEach(): Unit = {
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    else dir.mkdirs()
    
    // Initialize with a hyper-fast 200ms daemon interval for testing
    table = new AwanTable("test_daemon", 1000, testDir, daemonIntervalMs = 200)
    table.addColumn("id", isString = false)
    table.addColumn("val", isString = false)
  }

  override def afterEach(): Unit = {
    table.close()
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
  }

  test("Background Daemon should automatically compact blocks and reclaim memory (Happy Path)") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()
    for (i <- 100 until 200) table.insertRow(Array(i, i * 10))
    table.flush()

    table.blockManager.getLoadedBlocks.size shouldBe 2

    // Delete 40% (Breaches 30% threshold)
    for (i <- 10 until 50) table.delete(i)
    for (i <- 110 until 150) table.delete(i)

    // Wait for the Daemon to wake up
    var retries = 0
    while (table.blockManager.getLoadedBlocks.size > 1 && retries < 30) {
      Thread.sleep(100)
      retries += 1
    }

    table.blockManager.getLoadedBlocks.size shouldBe 1
    table.query("id", 15) shouldBe 0 
    table.query("id", 55) shouldBe 1 
  }

  test("Background Daemon should ignore blocks below the compaction threshold") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()
    for (i <- 100 until 200) table.insertRow(Array(i, i * 10))
    table.flush()

    table.blockManager.getLoadedBlocks.size shouldBe 2

    // Delete only 5% of the rows (Well below 30% threshold)
    for (i <- 0 until 5) table.delete(i)
    for (i <- 100 until 105) table.delete(i)

    // Wait for 1 full second (Daemon will wake up ~5 times)
    Thread.sleep(1000)

    // Verify the daemon checked the blocks but decided NOT to compact them
    table.blockManager.getLoadedBlocks.size shouldBe 2
  }

  test("Background Daemon should safely compact while main thread actively queries and inserts") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()
    for (i <- 100 until 200) table.insertRow(Array(i, i * 10))
    table.flush()

    // Delete 40% to guarantee the daemon WILL compact on its next wake-up
    for (i <- 10 until 50) table.delete(i)
    for (i <- 110 until 150) table.delete(i)

    // Start a concurrent Future that aggressively hammers the table with reads and writes
    // while the daemon is trying to swap block pointers in the background.
    val stressTest = Future {
      for (i <- 200 until 500) {
        table.insertRow(Array(i, i * 10))
        table.query("id", 55) shouldBe 1 // Keep reading a surviving row
        Thread.sleep(2) // Slight delay to ensure it overlaps with the daemon's 200ms interval
      }
    }

    // Wait for the stress test to finish (which gives the daemon plenty of time to run)
    Await.result(stressTest, 5.seconds)

    // Force a flush of the newly inserted rows from the stress test
    table.flush()

    // Verify the daemon successfully compacted the first two blocks
    // (Original 2 blocks -> 1 compacted block) + (Stress test flush -> 1 new block) = 2 blocks total
    table.blockManager.getLoadedBlocks.size shouldBe 2

    // Verify data integrity across the old compacted data AND the newly inserted data
    table.query("id", 15) shouldBe 0    // Deleted from old blocks
    table.query("id", 55) shouldBe 1    // Survived from old blocks
    table.query("id", 250) shouldBe 1   // Inserted during the stress test
  }
}