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

class AwanDBSpec extends AnyFlatSpec with Matchers {

  "The AwanDB Engine" should "maintain data consistency between Insert and Read" in {
    // 1. Setup
    val table = new AwanTable("test_products", 1000)
    table.addColumn("id")
    table.addColumn("price")
    
    val uniqueID = 99999
    
    // 2. Action: Insert
    table.insertRow(Array(uniqueID, 500))
    
    // 3. Assert
    val count = table.query(uniqueID - 1)
    
    // FIX: Use 'shouldBe' (One word) for Scala 3
    count shouldBe 1
    
    table.close()
  }

  it should "clear the Delta Buffer after a flush" in {
    val table = new AwanTable("test_flush", 1000)
    table.addColumn("val")
    
    table.insertRow(Array(123))
    
    // Check Pre-condition
    table.columns("val").deltaBuffer.nonEmpty shouldBe true
    
    // Action: Flush to Disk
    table.flush()
    
    // Check Post-condition
    table.columns("val").deltaBuffer.isEmpty shouldBe true
    table.close()
  }

  it should "survive a Save/Load cycle (Persistence)" in {
    val table = new AwanTable("test_persistence", 1000)
    table.addColumn("data")
    
    table.insertRow(Array(42))
    table.insertRow(Array(43))
    
    // Action: Flush triggers persistence
    table.flush()
    
    // Verify: Querying > 0 should find both rows from Disk
    val count = table.query(0)
    count shouldBe 2
    
    table.close()
  }

  it should "handle OOM gracefully" in {
    assertThrows[OutOfMemoryError] {
      NativeBridge.allocMainStore(10_000_000_000_000L)
    }
  }
}