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
import org.apache.arrow.vector.{IntVector, VarCharVector, Float4Vector}
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
    val colTypes = Array(0, 0)
    val dataPtrs = Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress())
    val offsetPtrs = Array(0L, 0L)
    val sizes = Array(rowCount * 4, rowCount * 4)
    val dims = Array(0, 0)

    table.bulkLoadFromArrowPointers(columnNames, colTypes, dataPtrs, offsetPtrs, sizes, dims, rowCount)

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
    val colTypes = Array(0, 0)
    val offsetPtrs = Array(0L, 0L)
    val sizes = Array(batchSize * 4, batchSize * 4)
    val dims = Array(0, 0)

    // BATCH 1
    idVector.allocateNew(batchSize)
    priceVector.allocateNew(batchSize)
    for (i <- 0 until batchSize) {
      idVector.setSafe(i, i)
      priceVector.setSafe(i, 100)
    }
    idVector.setValueCount(batchSize)
    priceVector.setValueCount(batchSize)

    val dataPtrs1 = Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress())
    table.bulkLoadFromArrowPointers(colNames, colTypes, dataPtrs1, offsetPtrs, sizes, dims, batchSize)

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

    val dataPtrs2 = Array(idVector.getDataBuffer.memoryAddress(), priceVector.getDataBuffer.memoryAddress())
    table.bulkLoadFromArrowPointers(colNames, colTypes, dataPtrs2, offsetPtrs, sizes, dims, batchSize)

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
    val reversedTypes = Array(0, 0)
    val reversedPointers = Array(priceVector.getDataBuffer.memoryAddress(), idVector.getDataBuffer.memoryAddress())
    val offsetPtrs = Array(0L, 0L)
    val sizes = Array(rowCount * 4, rowCount * 4)
    val dims = Array(0, 0)

    table.bulkLoadFromArrowPointers(reversedNames, reversedTypes, reversedPointers, offsetPtrs, sizes, dims, rowCount)

    table.countAll() shouldBe 5
    val row0 = table.getRow(0).get
    row0(0) shouldBe 0
    row0(1) shouldBe 999

    idVector.close()
    priceVector.close()
    allocator.close()
    table.close()
  }

  it should "zero-copy ingest Strings and AI Vectors (List[Float]) natively" in {
    val tableName = "bulk_ingress_complex"
    val dir = s"data/$tableName"
    clearDataDir(dir)

    val table = new AwanTable(tableName, 1000, dataDir = dir)
    table.addColumn("id", isString = false, isVector = false)
    table.addColumn("text", isString = true, isVector = false)
    table.addColumn("embed", isString = false, isVector = true)

    val allocator = new RootAllocator()
    val rowCount = 2
    
    // 1. Mock ID Column
    val idVec = new IntVector("id", allocator)
    idVec.allocateNew(rowCount)
    idVec.setSafe(0, 50)
    idVec.setSafe(1, 51)
    idVec.setValueCount(rowCount)

    // 2. Mock String Column (VarChar)
    val strVec = new VarCharVector("text", allocator)
    strVec.allocateNew(rowCount)
    strVec.setSafe(0, "hello".getBytes("UTF-8"))
    strVec.setSafe(1, "world_vector".getBytes("UTF-8")) // Long enough to trigger German string pool
    strVec.setValueCount(rowCount)

    // 3. Mock Inner Float Vector (Dimension = 3)
    val floatVec = new Float4Vector("embed_inner", allocator)
    val totalFloats = rowCount * 3
    floatVec.allocateNew(totalFloats)
    // Row 1
    floatVec.setSafe(0, 1.1f); floatVec.setSafe(1, 1.2f); floatVec.setSafe(2, 1.3f)
    // Row 2
    floatVec.setSafe(3, 2.1f); floatVec.setSafe(4, 2.2f); floatVec.setSafe(5, 2.3f)
    floatVec.setValueCount(totalFloats)

    // Construct the metadata arrays identical to AwanFlightSqlProducer
    val colNames = Array("id", "text", "embed")
    val colTypes = Array(0, 2, 3) // 0=Int, 2=String, 3=Vector
    val dataPtrs = Array(
        idVec.getDataBuffer.memoryAddress(), 
        strVec.getDataBuffer.memoryAddress(), 
        floatVec.getDataBuffer.memoryAddress()
    )
    val offsetPtrs = Array(0L, strVec.getOffsetBuffer.memoryAddress(), 0L)
    
    // String Size: (Rows * 16 byte header) + total byte pool
    val totalStrBytes = strVec.getOffsetBuffer.getInt(rowCount * 4L)
    val sizes = Array(rowCount * 4, (rowCount * 16) + totalStrBytes, totalFloats * 4)
    val dims = Array(0, 0, 3)

    // 🚀 EXECUTE TRUE ZERO-COPY INGRESS
    table.bulkLoadFromArrowPointers(colNames, colTypes, dataPtrs, offsetPtrs, sizes, dims, rowCount)

    // Assertions
    table.countAll() shouldBe 2
    
    // Check Row 1
    val row0 = table.getRow(50).get
    row0(0) shouldBe 50
    row0(2).asInstanceOf[Array[Float]] should contain inOrderOnly (1.1f, 1.2f, 1.3f)

    // Check Row 2
    val row1 = table.getRow(51).get
    row1(2).asInstanceOf[Array[Float]] should contain inOrderOnly (2.1f, 2.2f, 2.3f)

    idVec.close()
    strVec.close()
    floatVec.close()
    allocator.close()
    table.close()
  }
}