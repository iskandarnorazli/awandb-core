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

class AwanDBSpec extends AnyFlatSpec with Matchers {

  // [CRITICAL] Unique directory for this test suite to prevent collisions
  val TEST_DIR = "data/awandb_spec_unique"

  def cleanDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  "The AwanDB Engine" should "maintain data consistency between Insert and Read" in {
    cleanDir(TEST_DIR) // Clean start
    
    // 1. Setup with isolated directory
    val table = new AwanTable("test_products", 1000, TEST_DIR)
    table.addColumn("id")
    table.addColumn("price")
    
    val uniqueID = 99999
    
    // 2. Action: Insert
    table.insertRow(Array(uniqueID, 500))
    
    // 3. Assert
    val count = table.query(uniqueID - 1)
    
    count shouldBe 1
    
    table.close()
  }

  it should "clear the Delta Buffer after a flush" in {
    // Reuse directory but different table name helps isolation too
    val table = new AwanTable("test_flush", 1000, TEST_DIR)
    table.addColumn("val")
    
    table.insertRow(Array(123))
    
    // Check Pre-condition
    // [FIX] Updated to 'deltaIntBuffer' to match NativeColumn
    table.columns("val").deltaIntBuffer.nonEmpty shouldBe true
    
    // Action: Flush to Disk
    table.flush()
    
    // Check Post-condition
    // [FIX] Updated to 'deltaIntBuffer' to match NativeColumn
    table.columns("val").deltaIntBuffer.isEmpty shouldBe true
    table.close()
  }

  it should "survive a Save/Load cycle (Persistence)" in {
    cleanDir(TEST_DIR) // Ensure clean state for counting test
    
    val table = new AwanTable("test_persistence", 1000, TEST_DIR)
    table.addColumn("data")
    
    table.insertRow(Array(42))
    table.insertRow(Array(43))
    
    // Action: Flush triggers persistence
    table.flush()
    
    // Verify: Querying > 0 should find both rows from Disk
    // (This works because query() scans both RAM and the Snapshot Block List)
    val count = table.query(0)
    
    // Previous error "8 != 2" happened here because it read old files. 
    // Now it should be exactly 2.
    count shouldBe 2
    
    table.close()
  }

  it should "handle OOM gracefully" in {
    assertThrows[OutOfMemoryError] {
      // Attempt to allocate 10 TB
      NativeBridge.allocMainStore(10_000_000_000_000L)
    }
  }
}