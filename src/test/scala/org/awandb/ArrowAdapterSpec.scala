/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.awandb.core.query.VectorBatch
import org.awandb.core.jni.NativeBridge
import org.apache.arrow.vector.IntVector
import java.util.Arrays

import org.awandb.core.flight.ArrowAdapter

class ArrowAdapterSpec extends AnyFlatSpec with Matchers {

  "ArrowAdapter" should "zero-copy blast VectorBatch data into a 2-column Arrow VectorSchemaRoot" in {
    val allocator = new RootAllocator(Long.MaxValue)
    val batch = new VectorBatch(100, valueWidthBytes = 4)
    batch.count = 100
    
    val dummyKeys = (0 until 100).toArray
    val dummyVals = (1000 until 1100).toArray
    
    NativeBridge.loadData(batch.keysPtr, dummyKeys)
    NativeBridge.loadData(batch.valuesPtr, dummyVals)

    val keyField = new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), null)
    val valField = new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null)
    val schema = new Schema(Arrays.asList(keyField, valField))

    val root = ArrowAdapter.toVectorSchemaRoot(batch, schema, allocator)

    root.getRowCount shouldBe 100
    
    val arrowKeys = root.getVector("key").asInstanceOf[IntVector]
    val arrowVals = root.getVector("value").asInstanceOf[IntVector]

    arrowKeys.get(0) shouldBe 0
    arrowKeys.get(99) shouldBe 99
    arrowVals.get(0) shouldBe 1000
    arrowVals.get(99) shouldBe 1099

    // Cleanup
    root.close()
    allocator.getAllocatedMemory shouldBe 0 // Assert no Arrow memory leaks
    allocator.close()
    
    NativeBridge.freeMainStore(batch.keysPtr)
    NativeBridge.freeMainStore(batch.valuesPtr)
    NativeBridge.freeMainStore(batch.selectionVectorPtr)
  }

  it should "safely handle empty VectorBatches (0 rows)" in {
    val allocator = new RootAllocator(Long.MaxValue)
    val batch = new VectorBatch(100, valueWidthBytes = 4)
    batch.count = 0 // Represents a query that filtered out all rows

    val keyField = new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), null)
    val valField = new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null)
    val schema = new Schema(Arrays.asList(keyField, valField))

    val root = ArrowAdapter.toVectorSchemaRoot(batch, schema, allocator)

    root.getRowCount shouldBe 0
    root.getVector("key").getValueCount shouldBe 0
    root.getVector("value").getValueCount shouldBe 0

    // Cleanup
    root.close()
    allocator.getAllocatedMemory shouldBe 0
    allocator.close()
    
    NativeBridge.freeMainStore(batch.keysPtr)
    NativeBridge.freeMainStore(batch.valuesPtr)
    NativeBridge.freeMainStore(batch.selectionVectorPtr)
  }

  it should "handle single-column schemas gracefully" in {
    val allocator = new RootAllocator(Long.MaxValue)
    val batch = new VectorBatch(10, valueWidthBytes = 4)
    batch.count = 5 
    
    val dummyKeys = Array(10, 20, 30, 40, 50)
    NativeBridge.loadData(batch.keysPtr, dummyKeys)

    // Schema with ONLY one column
    val keyField = new Field("single_key", FieldType.notNullable(new ArrowType.Int(32, true)), null)
    val schema = new Schema(Arrays.asList(keyField))

    val root = ArrowAdapter.toVectorSchemaRoot(batch, schema, allocator)

    root.getRowCount shouldBe 5
    root.getSchema.getFields.size() shouldBe 1
    
    val arrowKeys = root.getVector("single_key").asInstanceOf[IntVector]
    arrowKeys.get(4) shouldBe 50

    // Cleanup
    root.close()
    allocator.getAllocatedMemory shouldBe 0
    allocator.close()
    
    NativeBridge.freeMainStore(batch.keysPtr)
    NativeBridge.freeMainStore(batch.valuesPtr)
    NativeBridge.freeMainStore(batch.selectionVectorPtr)
  }
}