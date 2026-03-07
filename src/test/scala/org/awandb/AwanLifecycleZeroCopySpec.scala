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
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import java.io.File

class AwanLifecycleZeroCopySpec extends AnyFlatSpec with Matchers {

  def clearDataDir(dir: String): Unit = {
    val f = new File(dir)
    if (f.exists()) {
      val files = f.listFiles()
      if (files != null) files.foreach(_.delete())
      f.delete()
    }
  }

  "Zero-Copy Pipeline" should "perfectly integrate with Primary Keys, Tombstones, and SQL Egress" in {
    val tableName = "lifecycle_zc_test"
    val dir = s"data/$tableName"
    clearDataDir(dir)

    val table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false)
    table.addColumn("val1", isString = false)
    SQLHandler.register(tableName, table)

    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    val val1Vector = new IntVector("val1", allocator)
    val rowCount = 10_000 
    
    // 1. Generate Data
    idVector.allocateNew(rowCount)
    val1Vector.allocateNew(rowCount)
    for (i <- 0 until rowCount) {
      idVector.setSafe(i, i) 
      val1Vector.setSafe(i, 500)
    }
    idVector.setValueCount(rowCount)
    val1Vector.setValueCount(rowCount)

    // 2. TRUE ZERO-COPY INGRESS
    val columnNames = Array("id", "val1")
    val rawPointers = Array(idVector.getDataBuffer.memoryAddress(), val1Vector.getDataBuffer.memoryAddress())
    table.bulkLoadFromArrowPointers(columnNames, rawPointers, rowCount)

    // --- LIFECYCLE VALIDATIONS ---

    // 3. Verify Primary Key Index mapping succeeded
    table.getRow(5000).isDefined shouldBe true
    table.getRow(5000).get(0) shouldBe 5000

    // 4. Verify Tombstones (Deletions) work on Zero-Copied memory
    SQLHandler.execute(s"DELETE FROM $tableName WHERE id = 5000")
    
    // PK Lookup should now fail
    table.getRow(5000).isDefined shouldBe false
    
    // Overall count should drop
    table.countAll() shouldBe 9999

    // 5. Verify SQL Egress strips Tombstones BEFORE the next Zero-Copy Egress
    // We expect 9,999 rows, and row ID 5000 MUST NOT be in the payload
    val queryResult = SQLHandler.execute(s"SELECT id FROM $tableName")
    queryResult.isError shouldBe false
    queryResult.affectedRows shouldBe 9999
    
    val returnedIds = queryResult.columnarData(0).asInstanceOf[Array[Int]]
    returnedIds.length shouldBe 9999
    returnedIds.contains(5000) shouldBe false // The ghost record is gone!

    idVector.close()
    val1Vector.close()
    allocator.close()
    table.close()
  }

  it should "safely survive engine restarts (Durability of Zero-Copied Blocks)" in {
    val tableName = "lifecycle_zc_durability"
    val dir = s"data/$tableName"
    clearDataDir(dir)

    // --- BOOT 1: Ingest via Zero-Copy ---
    var table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false)
    
    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    idVector.allocateNew(100)
    for (i <- 0 until 100) idVector.setSafe(i, i)
    idVector.setValueCount(100)

    table.bulkLoadFromArrowPointers(Array("id"), Array(idVector.getDataBuffer.memoryAddress()), 100)
    table.close() // Shut down the database

    // --- BOOT 2: Recover from Disk ---
    // The BlockManager should discover the .udb file created by the Zero-Copy ingest!
    table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false)
    
    table.countAll() shouldBe 100
    table.getRow(99).get(0) shouldBe 99

    idVector.close()
    allocator.close()
    table.close()
  }
}