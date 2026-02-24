/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.flight._
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import org.awandb.server.AwanFlightProducer

class DoPutStreamSpec extends AnyFunSuite with BeforeAndAfterAll {

  var allocator: RootAllocator = _
  var server: FlightServer = _
  var client: FlightClient = _

  override def beforeAll(): Unit = {
    allocator = new RootAllocator(Long.MaxValue)
    
    // Setup isolated tables for each test
    val tables = Seq("stream_basic", "stream_pressure", "stream_error")
    for (tName <- tables) {
      val table = new AwanTable(tName, 100_000, dataDir = s"target/data_$tName")
      table.addColumn("val", isString = false)
      SQLHandler.register(tName, table)
    }

    val location = Location.forGrpcInsecure("localhost", 33333)
    server = FlightServer.builder(allocator, location, new AwanFlightProducer(allocator, location)).build()
    server.start()

    client = FlightClient.builder(allocator, location).build()
  }

  override def afterAll(): Unit = {
    client.close()
    server.close()
    allocator.close()
  }

  test("1. HTAP Feature: Basic DoPut Stream Ingestion") {
    val tableName = "stream_basic"
    val intVector = new IntVector("val", allocator)
    intVector.allocateNew(1000)
    for (i <- 0 until 1000) intVector.setSafe(i, i * 10)
    intVector.setValueCount(1000)
    
    val root = new VectorSchemaRoot(
      java.util.Arrays.asList(intVector.getField), 
      java.util.Arrays.asList(intVector)
    )
    root.setRowCount(1000)

    val clientStream = client.startPut(FlightDescriptor.path(tableName), root, new SyncPutListener())
    clientStream.putNext()
    clientStream.completed()

    Thread.sleep(100) // Allow Fusion Queue to drain
    
    val countRes = SQLHandler.execute(s"SELECT COUNT(*) FROM $tableName")
    assert(!countRes.isError)
    assert(countRes.message.contains("1000"), "Failed to ingest single batch.")
    
    root.close()
    intVector.close() // [FIX] Free the raw vector memory
  }

  test("2. HTAP Feature: Sustained Stream Pressure (100,000 rows)") {
    val tableName = "stream_pressure"
    val batchSize = 1000
    val totalBatches = 100
    
    val intVector = new IntVector("val", allocator)
    intVector.allocateNew(batchSize)
    
    val root = new VectorSchemaRoot(
      java.util.Arrays.asList(intVector.getField), 
      java.util.Arrays.asList(intVector)
    )

    val listener = new SyncPutListener()
    val clientStream = client.startPut(FlightDescriptor.path(tableName), root, listener)

    // Blast 100 consecutive batches over the open network socket
    for (batchIdx <- 0 until totalBatches) {
      for (i <- 0 until batchSize) {
        intVector.setSafe(i, (batchIdx * batchSize) + i)
      }
      intVector.setValueCount(batchSize)
      root.setRowCount(batchSize)
      clientStream.putNext()
    }
    
    clientStream.completed()
    listener.getResult() // Wait for server to process

    Thread.sleep(500) // Allow heavy Fusion Queue to drain fully
    
    val countRes = SQLHandler.execute(s"SELECT COUNT(*) FROM $tableName")
    assert(!countRes.isError)
    assert(countRes.message.contains("100000"), s"Lost rows during sustained pressure! Output: ${countRes.message}")
    
    root.close()
    intVector.close() // [FIX] Free the raw vector memory
  }

  test("3. Security/Stability: Reject Invalid Stream Schema Gracefully") {
    val tableName = "stream_error"
    
    // Malicious/Mistaken Client tries to stream Strings into our Int column
    val strVector = new VarCharVector("val", allocator)
    strVector.allocateNew(10)
    strVector.setSafe(0, "HACKER".getBytes("UTF-8"))
    strVector.setValueCount(1)
    
    val root = new VectorSchemaRoot(
      java.util.Arrays.asList(strVector.getField), 
      java.util.Arrays.asList(strVector)
    )
    root.setRowCount(1)

    val listener = new SyncPutListener()
    val clientStream = client.startPut(FlightDescriptor.path(tableName), root, listener)

    clientStream.putNext()
    clientStream.completed()
    
    // We expect the FlightRuntimeException because AwanFlightProducer explicitly throws INVALID_ARGUMENT
    val thrown = intercept[FlightRuntimeException] {
      listener.getResult()
    }
    
    assert(thrown.status().code() == FlightStatusCode.INVALID_ARGUMENT, "Server did not return INVALID_ARGUMENT")
    assert(thrown.getMessage.contains("Only IntVector streams are currently supported"), "Server returned wrong error message")
    
    // Verify the engine didn't crash and the table is still empty
    val countRes = SQLHandler.execute(s"SELECT COUNT(*) FROM $tableName")
    assert(countRes.message.contains("NULL") || countRes.message.contains("0"), "Data polluted by invalid stream")
    
    root.close()
    strVector.close() // [FIX] Free the malicious string vector memory
  }
}