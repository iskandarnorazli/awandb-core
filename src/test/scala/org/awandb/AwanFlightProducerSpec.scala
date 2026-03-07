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

package org.awandb.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.flight._
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import java.util.Collections
import scala.jdk.CollectionConverters._

class AwanFlightProducerSpec extends AnyFunSuite with MockitoSugar {

  test("acceptPut should safely ingest mixed Arrow vectors, apply padding, and run within an epoch") {
    val allocator = new RootAllocator()
    
    // 1. Setup Mock Table in SQLHandler
    val tableName = "flight_test_docs"
    val table = new AwanTable(tableName, 1000, dataDir = "data/flight_test")
    table.addColumn("id", isString = false)
    table.addColumn("content", isString = true)
    table.addColumn("embedding", isVector = true)
    table.addColumn("missing_col", isString = false) // To test padding
    SQLHandler.register(tableName, table)

    // 2. Build Arrow Vectors manually
    val idVector = new IntVector("id", allocator)
    idVector.allocateNew(2)
    idVector.setSafe(0, 101)
    idVector.setSafe(1, 102)
    idVector.setValueCount(2)

    val contentVector = new VarCharVector("content", allocator)
    contentVector.allocateNew(2)
    contentVector.setSafe(0, "Document A".getBytes("UTF-8"))
    contentVector.setSafe(1, "Document B".getBytes("UTF-8"))
    contentVector.setValueCount(2)

    // Build Vector Embeddings (List[Float])
    val listVector = ListVector.empty("embedding", allocator)
    val writer: UnionListWriter = listVector.getWriter
    writer.allocate()
    
    // Row 0: [0.1f, 0.2f]
    writer.setPosition(0)
    writer.startList()
    writer.float4().writeFloat4(0.1f)
    writer.float4().writeFloat4(0.2f)
    writer.endList()
    
    // Row 1: [0.3f, 0.4f]
    writer.setPosition(1)
    writer.startList()
    writer.float4().writeFloat4(0.3f)
    writer.float4().writeFloat4(0.4f)
    writer.endList()
    listVector.setValueCount(2)

    // 3. Assemble VectorSchemaRoot
    val schema = new Schema(List(
      idVector.getField, 
      contentVector.getField, 
      listVector.getField
    ).asJava)
    
    val root = new VectorSchemaRoot(schema, java.util.Arrays.asList(idVector, contentVector, listVector), 2)

    // 4. Mock the FlightStream
    val mockStream = mock[FlightStream]
    val mockDescriptor = FlightDescriptor.path(tableName)
    when(mockStream.getDescriptor).thenReturn(mockDescriptor)
    when(mockStream.getRoot).thenReturn(root)
    
    // Simulate stream returning 1 batch, then finishing
    when(mockStream.next()).thenReturn(true).thenReturn(false)

    val mockContext = mock[FlightProducer.CallContext]
    val mockListener = mock[FlightProducer.StreamListener[PutResult]]

    // 5. Execute acceptPut
    val producer = new AwanFlightSqlProducer(allocator, null)
    val runnable = producer.acceptPut(mockContext, mockStream, mockListener)
    runnable.run()

    // 6. Assertions & Validation
    // Verify Flight Stream completed successfully
    verify(mockListener, times(1)).onCompleted()
    
    // Verify Data in Table
    val row1 = table.getRow(101).get
    assert(row1(0) == 101, "ID should be 101")
    assert(row1(1) == "Document A", "String should be parsed")
    assert(row1(2).asInstanceOf[Array[Float]].sameElements(Array(0.1f, 0.2f)), "Vector should match")
    assert(row1(3) == 0, "Missing column should be padded with default int (0)")

    val row2 = table.getRow(102).get
    assert(row2(1) == "Document B")

    // Cleanup
    root.close()
    table.close()
    allocator.close()
  }
}