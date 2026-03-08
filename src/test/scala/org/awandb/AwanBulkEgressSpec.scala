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
import org.awandb.core.jni.NativeBridge

class AwanBulkEgressSpec extends AnyFlatSpec with Matchers {

  "Egress Pipeline" should "zero-copy blast a massive Scala Array[Int] directly into Arrow Off-Heap Memory" in {
    val allocator = new RootAllocator()
    val intVector = new IntVector("price", allocator)
    val rowCount = 1_000_000

    // 1. Mock the Engine Output (e.g., SELECT price FROM table)
    val engineResultArray = new Array[Int](rowCount)
    for (i <- 0 until rowCount) {
      engineResultArray(i) = i * 42 
    }

    // 2. Allocate the empty Arrow Off-Heap Vector
    intVector.allocateNew(rowCount)

    // 3. 🚀 TRUE ZERO-COPY EGRESS
    val arrowDataPtr = intVector.getDataBuffer.memoryAddress()
    NativeBridge.loadData(arrowDataPtr, engineResultArray)

    // 4. Fast-path Validity Buffer
    var i = 0
    while (i < rowCount) {
      intVector.setIndexDefined(i)
      i += 1
    }
    intVector.setValueCount(rowCount)

    // 5. Assertions
    intVector.getValueCount shouldBe 1_000_000
    intVector.get(0) shouldBe 0
    intVector.get(10) shouldBe 420
    intVector.get(500000) shouldBe 21000000
    intVector.get(999999) shouldBe 999999 * 42

    intVector.close()
    allocator.close()
  }

  it should "safely egress Strings and handle NULL values without crashing" in {
    val allocator = new RootAllocator()
    val strVector = new VarCharVector("name", allocator)
    val rowCount = 5

    // Mock engine output with a deliberate null value
    val engineResultArray = Array("Alice", "Bob", null, "David", "")

    strVector.allocateNew(rowCount)
    
    // Simulate the AwanFlightSqlProducer string egress loop
    var i = 0
    while (i < rowCount) {
      val s = if (engineResultArray(i) == null) "" else engineResultArray(i)
      strVector.setSafe(i, s.getBytes("UTF-8"))
      i += 1
    }
    strVector.setValueCount(rowCount)

    // Assertions
    strVector.getValueCount shouldBe 5
    new String(strVector.get(0), "UTF-8") shouldBe "Alice"
    new String(strVector.get(2), "UTF-8") shouldBe "" // Null was safely coalesced
    new String(strVector.get(4), "UTF-8") shouldBe "" // Empty string preserved

    strVector.close()
    allocator.close()
  }

  it should "safely handle empty result sets (Zero Rows) gracefully" in {
    val allocator = new RootAllocator()
    val intVector = new IntVector("empty_col", allocator)
    val rowCount = 0

    val engineResultArray = new Array[Int](rowCount)

    intVector.allocateNew(rowCount)
    
    // Should not crash or segfault when ptr and array are empty
    val arrowDataPtr = intVector.getDataBuffer.memoryAddress()
    NativeBridge.loadData(arrowDataPtr, engineResultArray)

    intVector.setValueCount(0)

    intVector.getValueCount shouldBe 0

    intVector.close()
    allocator.close()
  }

  it should "align multiple columns perfectly (Int + String Mixed Workload)" in {
    val allocator = new RootAllocator()
    val idVector = new IntVector("id", allocator)
    val nameVector = new VarCharVector("name", allocator)
    val rowCount = 3

    // Mock engine outputs
    val idResultArray = Array(100, 200, 300)
    val nameResultArray = Array("Product A", "Product B", "Product C")

    // Allocate Arrow memory
    idVector.allocateNew(rowCount)
    nameVector.allocateNew(rowCount)

    // 1. Egress Ints (Zero-Copy)
    NativeBridge.loadData(idVector.getDataBuffer.memoryAddress(), idResultArray)
    var i = 0
    while (i < rowCount) {
      idVector.setIndexDefined(i)
      i += 1
    }
    idVector.setValueCount(rowCount)

    // 2. Egress Strings
    var j = 0
    while (j < rowCount) {
      nameVector.setSafe(j, nameResultArray(j).getBytes("UTF-8"))
      j += 1
    }
    nameVector.setValueCount(rowCount)

    // Assert multi-column row alignment
    idVector.get(1) shouldBe 200
    new String(nameVector.get(1), "UTF-8") shouldBe "Product B"

    idVector.get(2) shouldBe 300
    new String(nameVector.get(2), "UTF-8") shouldBe "Product C"

    idVector.close()
    nameVector.close()
    allocator.close()
  }

  it should "safely egress AI Vectors (Array[Float] into Arrow ListVector)" in {
    val allocator = new RootAllocator()
    
    // Create an Arrow ListVector of Floats
    val listVector = org.apache.arrow.vector.complex.ListVector.empty("embeddings", allocator)
    val innerFloatVec = listVector.addOrGetVector[org.apache.arrow.vector.Float4Vector](
      org.apache.arrow.vector.types.pojo.FieldType.nullable(
        // [FIX] Removed .pojo from the FloatingPointPrecision path
        new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)
      )
    ).getVector

    val rowCount = 2
    val dim = 3
    
    // Mock the Array[Array[Float]] output from AwanTable.getRow()
    val engineOutput = Array(
        Array(0.1f, 0.2f, 0.3f),
        Array(0.9f, 0.8f, 0.7f)
    )

    listVector.allocateNew()
    innerFloatVec.allocateNew(rowCount * dim)

    // Simulate the AwanFlightSqlProducer Vector packing loop
    var i = 0
    var floatOffset = 0
    while (i < rowCount) {
        val vec = engineOutput(i)
        
        // Write Offsets
        listVector.getOffsetBuffer.setInt(i * 4L, floatOffset)
        
        // Write Floats
        var j = 0
        while (j < dim) {
            innerFloatVec.setSafe(floatOffset + j, vec(j))
            j += 1
        }
        
        listVector.getOffsetBuffer.setInt((i + 1) * 4L, floatOffset + dim)
        listVector.setNotNull(i)
        
        floatOffset += dim
        i += 1
    }
    
    innerFloatVec.setValueCount(floatOffset)
    listVector.setValueCount(rowCount)

    // Assertions
    listVector.getValueCount shouldBe 2
    innerFloatVec.getValueCount shouldBe 6
    innerFloatVec.get(4) shouldBe 0.8f

    listVector.close()
    allocator.close()
  }
}