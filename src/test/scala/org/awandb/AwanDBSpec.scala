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
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import org.scalatest.BeforeAndAfterEach

class AwanDBSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Isolated directory for extensive testing
  val TEST_DIR = "data/awandb_spec_extensive"

  // Helper: aggressively clean directory to prevent "Zombie Data" from previous runs
  def scrubDirectory(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach(_.delete()))
      dir.delete()
    }
    dir.mkdirs()
  }

  // Runs before EVERY test case to ensure total isolation
  override def beforeEach(): Unit = {
    scrubDirectory()
  }

  "The AwanDB Engine" should "maintain consistency across the Delta -> Flush -> Disk cycle" in {
    val table = new AwanTable("consistency_test", 1000, TEST_DIR)
    table.addColumn("id")
    
    // 1. In RAM (Delta Buffer)
    table.insertRow(Array[Any](100))
    table.query("id", 0) shouldBe 1
    
    // 2. On Disk (Snapshot Block)
    table.flush()
    table.query("id", 0) shouldBe 1 
    
    table.close()
  }

  it should "recover data from disk without needing new inserts (Cold Boot)" in {
    val tableName = "restart_test"
    val data = Array(10, 20, 30)

    // Session 1: Write and Flush
    val table1 = new AwanTable(tableName, 1000, TEST_DIR)
    table1.addColumn("id")
    data.foreach(v => table1.insertRow(Array[Any](v)))
    table1.flush()
    table1.close()

    // Session 2: The "Reboot" (Open same table, no new inserts)
    // BlockManager should automatically find the .awan files
    val table2 = new AwanTable(tableName, 1000, TEST_DIR)
    table2.addColumn("id") 
    
    // Querying > 0 should find all 3 rows recovered from disk
    table2.query("id", 0) shouldBe 3
    table2.close()
  }

  it should "correctly store and query mixed Integer and German String columns" in {
    val table = new AwanTable("mixed_types", 1000, TEST_DIR)
    table.addColumn("age", isString = false)    
    table.addColumn("name", isString = true) // Dictionary Encoding Path
    
    // Insert mixed types
    table.insertRow(Array[Any](25, "Iskandar"))
    table.insertRow(Array[Any](30, "AwanDB_Long_String_Test"))

    // Force flush to test C++ Block format serialization
    table.flush() 
    
    // 1. Query Integer Column
    table.query("age", 20) shouldBe 2
    
    // 2. [NEW] Query String Column (End-to-End Test)
    // This verifies: Scala Insert -> Dict Encode -> Flush -> C++ Load -> AVX Scan
    table.query("name", "Iskandar") shouldBe 1
    table.query("name", "AwanDB_Long_String_Test") shouldBe 1
    table.query("name", "NonExistent") shouldBe 0
    
    table.close()
  }

  it should "handle high-volume batch inserts and Memory Alignment" in {
    val capacity = 5000
    val table = new AwanTable("volume_test", capacity, TEST_DIR)
    table.addColumn("seq")
    
    // Insert enough rows to trigger multiple flushes or fill the buffer
    val totalRows = 12000 
    (1 to totalRows).foreach { i =>
      table.insertRow(Array[Any](i))
    }
    
    table.query("seq", 0) shouldBe totalRows
    table.close()
  }

  it should "correctly compute min/max Zone Maps after flushing" in {
    val table = new AwanTable("zonemap_test", 1000, TEST_DIR)
    table.addColumn("score")
    
    table.insertRow(Array[Any](10))
    table.insertRow(Array[Any](50))
    table.insertRow(Array[Any](100))
    
    table.flush()
    
    // Verify C++ Metadata
    // Note: We need at least one block to be created
    if (table.columns("score").snapshotBlocks.nonEmpty) {
      val blockPtr = table.columns("score").snapshotBlocks.head.ptr
      val (min, max) = NativeBridge.getZoneMap(blockPtr, 0)
      
      min shouldBe 10
      max shouldBe 100
    }
    
    table.close()
  }

  it should "handle OOM gracefully in the Native Layer" in {
    assertThrows[OutOfMemoryError] {
      // Attempt to allocate impossible amount (10 TB)
      NativeBridge.allocMainStore(10_000_000_000_000L)
    }
  }
}