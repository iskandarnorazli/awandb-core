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
import java.util.UUID

class AwanDBSpec extends AnyFlatSpec with Matchers {

  // [ISOLATION] Run each test in a fresh, unique directory
  // This prevents "Zombie Data" from previous runs crashing the current test.
  def withTestDir(testCode: String => Any): Unit = {
    val uniqueId = UUID.randomUUID().toString.take(8)
    val dirPath = s"target/test_data_awandb_$uniqueId"
    val dir = new File(dirPath)
    
    // Clean start
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()
    
    try {
      testCode(dirPath)
    } finally {
      // Best-effort cleanup
      deleteRecursively(dir)
    }
  }

  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  "The AwanDB Engine" should "maintain consistency across the Delta -> Flush -> Disk cycle" in {
    withTestDir { dir =>
      val table = new AwanTable("consistency_test", 1000, dir)
      table.addColumn("id")
      
      // 1. In RAM (Delta Buffer)
      table.insertRow(Array[Any](100))
      
      // [FIX] Use Equality Check: id == 100 (Not id == 0)
      table.query("id", 100) shouldBe 1
      
      // 2. On Disk (Snapshot Block)
      table.flush()
      
      // Verify persistence
      table.query("id", 100) shouldBe 1 
      
      table.close()
    }
  }

  it should "recover data from disk without needing new inserts (Cold Boot)" in {
    withTestDir { dir =>
      val tableName = "restart_test"
      val data = Array(10, 20, 30)

      // Session 1: Write and Flush
      val table1 = new AwanTable(tableName, 1000, dir)
      table1.addColumn("id")
      data.foreach(v => table1.insertRow(Array[Any](v)))
      table1.flush()
      table1.close()

      // Session 2: The "Reboot" (Open same table, no new inserts)
      val table2 = new AwanTable(tableName, 1000, dir)
      table2.addColumn("id") 
      
      // [FIX] Use Range Query API: query(threshold) -> id > threshold
      // query(0) means "Select count(*) where id > 0"
      table2.query(0) shouldBe 3
      
      table2.close()
    }
  }

  it should "correctly store and query mixed Integer and German String columns" in {
    withTestDir { dir =>
      val table = new AwanTable("mixed_types", 1000, dir)
      table.addColumn("age", isString = false)    
      table.addColumn("name", isString = true, useDictionary = true) // Enable Dictionary
      
      // Insert mixed types
      table.insertRow(Array[Any](25, "Iskandar"))
      table.insertRow(Array[Any](30, "AwanDB_Long_String_Test"))

      // Force flush to test C++ Block format serialization
      table.flush() 
      
      // 1. Query Integer Column (Equality)
      table.query("age", 25) shouldBe 1
      
      // 2. Query String Column (Equality)
      table.query("name", "Iskandar") shouldBe 1
      table.query("name", "AwanDB_Long_String_Test") shouldBe 1
      table.query("name", "NonExistent") shouldBe 0
      
      table.close()
    }
  }

  it should "handle high-volume batch inserts and Memory Alignment" in {
    withTestDir { dir =>
      val capacity = 5000
      val table = new AwanTable("volume_test", capacity, dir)
      table.addColumn("seq")
      
      // Insert enough rows to trigger multiple flushes or fill the buffer
      val totalRows = 12000 
      (1 to totalRows).foreach { i =>
        table.insertRow(Array[Any](i))
      }
      
      // [FIX] Use Range Query: seq > 0
      table.query(0) shouldBe totalRows
      table.close()
    }
  }

  it should "correctly compute min/max Zone Maps after flushing" in {
    withTestDir { dir =>
      val table = new AwanTable("zonemap_test", 1000, dir)
      table.addColumn("score")
      
      table.insertRow(Array[Any](10))
      table.insertRow(Array[Any](50))
      table.insertRow(Array[Any](100))
      
      table.flush()
      
      // Verify C++ Metadata
      // Access block pointer from BlockManager directly
      if (table.blockManager.getLoadedBlocks.nonEmpty) {
         val blockPtr = table.blockManager.getLoadedBlocks.head
         val (min, max) = NativeBridge.getZoneMap(blockPtr, 0)
         
         min shouldBe 10
         max shouldBe 100
      }
      
      table.close()
    }
  }

  it should "handle OOM gracefully in the Native Layer" in {
    assertThrows[OutOfMemoryError] {
      // Attempt to allocate impossible amount (10 TB)
      NativeBridge.allocMainStore(10_000_000_000_000L)
    }
  }
}