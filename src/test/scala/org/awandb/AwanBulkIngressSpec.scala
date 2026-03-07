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
import java.io.File

class AwanBulkIngressSpec extends AnyFlatSpec with Matchers {

  // Helper to wipe test data between runs
  def clearDataDir(dir: String): Unit = {
    val f = new File(dir)
    if (f.exists()) {
      val files = f.listFiles()
      if (files != null) files.foreach(_.delete())
      f.delete()
    }
  }

  "AwanTable" should "bulk load multiple Arrow column pointers directly into a C++ block" in {
    val tableName = "bulk_ingress_test_1"
    val dir = s"data/$tableName"
    clearDataDir(dir) // Wipe old data!

    val table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false, isVector = false)
    table.addColumn("price", isString = false, isVector = false)

    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    val priceVector = new IntVector("price", allocator)
    val rowCount = 1_000_000 
    
    idVector.allocateNew(rowCount)
    priceVector.allocateNew(rowCount)
    for (i <- 0 until rowCount) {
      idVector.setSafe(i, i) 
      priceVector.setSafe(i, i * 10)
    }
    idVector.setValueCount(rowCount)
    priceVector.setValueCount(rowCount)

    val columnNames = Array("id", "price")
    val rawPointers = Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress())

    table.bulkLoadFromArrowPointers(columnNames, rawPointers, rowCount)

    table.countAll() shouldBe 1_000_000
    
    val row0 = table.getRow(0).get
    row0(0) shouldBe 0
    row0(1) shouldBe 0

    val rowLast = table.getRow(999999).get
    rowLast(0) shouldBe 999999
    rowLast(1) shouldBe 9999990

    idVector.close()
    priceVector.close()
    allocator.close()
    table.close()
  }

  it should "safely append multiple consecutive bulk RecordBatches" in {
    val tableName = "bulk_ingress_test_stream"
    val dir = s"data/$tableName"
    clearDataDir(dir) // Wipe old data!

    val table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false)
    table.addColumn("price", isString = false)

    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    val priceVector = new IntVector("price", allocator)
    val batchSize = 500

    val colNames = Array("id", "price")

    // BATCH 1
    idVector.allocateNew(batchSize)
    priceVector.allocateNew(batchSize)
    for (i <- 0 until batchSize) {
      idVector.setSafe(i, i)
      priceVector.setSafe(i, 100)
    }
    idVector.setValueCount(batchSize)
    priceVector.setValueCount(batchSize)

    table.bulkLoadFromArrowPointers(colNames, Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress()), batchSize)

    // BATCH 2
    idVector.clear()
    priceVector.clear()
    idVector.allocateNew(batchSize)
    priceVector.allocateNew(batchSize)
    for (i <- 0 until batchSize) {
      idVector.setSafe(i, i + 500)
      priceVector.setSafe(i, 200)
    }
    idVector.setValueCount(batchSize)
    priceVector.setValueCount(batchSize)

    table.bulkLoadFromArrowPointers(colNames, Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress()), batchSize)

    table.countAll() shouldBe 1000
    table.getRow(0).get(1) shouldBe 100
    table.getRow(499).get(1) shouldBe 100
    table.getRow(500).get(1) shouldBe 200
    table.getRow(999).get(1) shouldBe 200

    idVector.close()
    priceVector.close()
    allocator.close()
    table.close()
  }

  it should "automatically realign columns if Arrow sends them out of order" in {
    val tableName = "bulk_ingress_test_align"
    val dir = s"data/$tableName"
    clearDataDir(dir) // Wipe old data!

    val table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false)
    table.addColumn("price", isString = false)

    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    val priceVector = new IntVector("price", allocator)
    val rowCount = 5

    idVector.allocateNew(rowCount)
    priceVector.allocateNew(rowCount)
    for (i <- 0 until rowCount) {
      idVector.setSafe(i, i) 
      priceVector.setSafe(i, 999) 
    }
    idVector.setValueCount(rowCount)
    priceVector.setValueCount(rowCount)

    val reversedNames = Array("price", "id")
    val reversedPointers = Array(priceVector.getDataBuffer.memoryAddress(), idVector.getDataBuffer.memoryAddress())

    table.bulkLoadFromArrowPointers(reversedNames, reversedPointers, rowCount)

    table.countAll() shouldBe 5
    val row0 = table.getRow(0).get
    row0(0) shouldBe 0
    row0(1) shouldBe 999

    idVector.close()
    priceVector.close()
    allocator.close()
    table.close()
  }
}