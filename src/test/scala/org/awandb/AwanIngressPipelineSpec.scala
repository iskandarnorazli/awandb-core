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
import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.awandb.core.engine.AwanTable

class AwanIngressPipelineSpec extends AnyFlatSpec with Matchers {

  "AwanTable" should "ingest Arrow off-heap memory directly via C++ pointers" in {
    val tableName = "ingress_test_1"
    val table = new AwanTable(tableName, 1000, dataDir = s"data/$tableName")
    table.addColumn("id", isString = false, isVector = false)

    val allocator = new RootAllocator()
    val intVector = new IntVector("id", allocator)
    val rowCount = 100
    
    intVector.allocateNew(rowCount)
    for (i <- 0 until rowCount) {
      intVector.setSafe(i, i * 10) 
    }
    intVector.setValueCount(rowCount)

    val rawPointer = intVector.getDataBuffer.memoryAddress()
    table.insertBatchFromPointer("id", rawPointer, rowCount)

    table.countAll() shouldBe 100
    table.getRow(0).get.head shouldBe 0
    table.getRow(500).get.head shouldBe 500

    intVector.close()
    allocator.close()
    table.close()
  }

  it should "safely append multiple continuous Arrow RecordBatches (Stream Simulation)" in {
    val tableName = "ingress_test_stream"
    val table = new AwanTable(tableName, 1000, dataDir = s"data/$tableName")
    table.addColumn("id", isString = false, isVector = false)

    val allocator = new RootAllocator()
    val intVector = new IntVector("id", allocator)
    val batchSize = 50

    // BATCH 1 (IDs 0 to 49)
    intVector.allocateNew(batchSize)
    for (i <- 0 until batchSize) intVector.setSafe(i, i)
    intVector.setValueCount(batchSize)
    
    table.insertBatchFromPointer("id", intVector.getDataBuffer.memoryAddress(), batchSize)

    // BATCH 2 (IDs 50 to 99)
    intVector.clear()
    intVector.allocateNew(batchSize)
    for (i <- 0 until batchSize) intVector.setSafe(i, i + 50)
    intVector.setValueCount(batchSize)

    table.insertBatchFromPointer("id", intVector.getDataBuffer.memoryAddress(), batchSize)

    // Verify both batches merged seamlessly and the index offsets shifted correctly
    table.countAll() shouldBe 100
    
    // Check boundaries
    table.getRow(0).get.head shouldBe 0
    table.getRow(49).get.head shouldBe 49
    table.getRow(50).get.head shouldBe 50
    table.getRow(99).get.head shouldBe 99

    intVector.close()
    allocator.close()
    table.close()
  }

  it should "handle mixed schema workloads (Off-Heap Ints + Standard Strings)" in {
    val tableName = "ingress_test_mixed"
    val table = new AwanTable(tableName, 1000, dataDir = s"data/$tableName")
    table.addColumn("id", isString = false)
    table.addColumn("name", isString = true)

    val allocator = new RootAllocator()
    val intVector = new IntVector("id", allocator)
    val strVector = new VarCharVector("name", allocator)
    val rowCount = 5

    // Populate Vectors
    intVector.allocateNew(rowCount)
    strVector.allocateNew(rowCount)
    for (i <- 0 until rowCount) {
      intVector.setSafe(i, i + 1)
      strVector.setSafe(i, s"User_$i".getBytes("UTF-8"))
    }
    intVector.setValueCount(rowCount)
    strVector.setValueCount(rowCount)

    // 1. Ingest Strings (Simulating the JVM loop in AwanFlightSqlProducer)
    var i = 0
    while (i < rowCount) {
      val bytes = strVector.get(i)
      val strVal = if (bytes == null) "" else new String(bytes, "UTF-8")
      table.columns("name").insert(strVal)
      i += 1
    }

    // 2. Ingest Ints (Zero-Copy)
    table.insertBatchFromPointer("id", intVector.getDataBuffer.memoryAddress(), rowCount)

    // Assertions
    table.countAll() shouldBe 5
    val row3 = table.getRow(3).get
    row3(0) shouldBe 3
    row3(1) shouldBe "User_2" // ID 3 corresponds to index 2

    intVector.close()
    strVector.close()
    allocator.close()
    table.close()
  }
}