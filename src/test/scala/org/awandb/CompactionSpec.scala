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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.memory.{EpochManager, MemoryReleaser}
import scala.collection.mutable.ArrayBuffer

class CompactionSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  class MockMemoryReleaser extends MemoryReleaser {
    val freedPointers = ArrayBuffer[Long]()
    override def free(ptr: Long): Unit = synchronized {
      freedPointers.append(ptr)
    }
  }

  var table: AwanTable = _
  var epochManager: EpochManager = _
  var releaser: MockMemoryReleaser = _ 

  override def beforeEach(): Unit = {
    releaser = new MockMemoryReleaser()
    epochManager = new EpochManager(releaser)
    
    val dir = new java.io.File("target/data_compaction")
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    else dir.mkdirs()
    
    table = new AwanTable("test_compaction", 1000, dataDir = "target/data_compaction")
    table.addColumn("id", isString = false)
    table.addColumn("val", isString = false)
  }

  override def afterEach(): Unit = {
    table.close()
    val dir = new java.io.File("target/data_compaction")
    if (dir.exists()) dir.listFiles().foreach(_.delete())
  }

  test("Compactor should merge blocks and retire old pointers to EpochManager") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()

    for (i <- 100 until 200) table.insertRow(Array(i, i * 10))
    table.flush()

    // Delete 40%
    for (i <- 10 until 50) table.delete(i)
    for (i <- 110 until 150) table.delete(i)

    val compactor = new Compactor(table, epochManager)
    val compactedBlocks = compactor.compact(threshold = 0.3)
    
    compactedBlocks shouldBe 2 
    table.blockManager.getLoadedBlocks.size shouldBe 1

    table.query("id", 15) shouldBe 0 
    table.query("id", 55) shouldBe 1 

    epochManager.getPendingReclaims shouldBe 4 // 2 blocks + 2 bitmasks
  }

  test("Compactor should ignore blocks below the deletion threshold") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()

    // Delete only 5 rows (5%)
    for (i <- 0 until 5) table.delete(i)

    val compactor = new Compactor(table, epochManager)
    // Set threshold to 30%. It should skip this block.
    val compactedBlocks = compactor.compact(threshold = 0.3)
    
    compactedBlocks shouldBe 0
    table.blockManager.getLoadedBlocks.size shouldBe 1
  }

  test("Compactor should safely drop blocks that are 100% deleted") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()

    // Tombstone the entire block
    for (i <- 0 until 100) table.delete(i)

    val compactor = new Compactor(table, epochManager)
    val compactedBlocks = compactor.compact(threshold = 0.3)
    
    compactedBlocks shouldBe 1
    // The block should be completely gone, no new block created!
    table.blockManager.getLoadedBlocks.size shouldBe 0 
    
    epochManager.getPendingReclaims shouldBe 2 // 1 block + 1 bitmask
  }

  test("Compactor should update the Primary Index for surviving rows") {
    for (i <- 0 until 100) table.insertRow(Array(i, i * 10))
    table.flush()

    for (i <- 100 until 200) table.insertRow(Array(i, i * 10))
    table.flush()

    // Delete a few rows to trigger compaction
    for (i <- 0 until 40) table.delete(i)
    for (i <- 100 until 140) table.delete(i)

    val compactor = new Compactor(table, epochManager)
    compactor.compact(threshold = 0.3)

    // ID 50 survived and was moved to a completely new Block/Memory Address.
    // If the Primary Index wasn't updated, this getRow() will return None or garbage!
    val rowOption = table.getRow(50)
    rowOption.isDefined shouldBe true
    rowOption.get(0) shouldBe 50
    rowOption.get(1) shouldBe 500

    // Try deleting a surviving row after it moved
    val deletedSuccessfully = table.delete(150)
    deletedSuccessfully shouldBe true
  }
}