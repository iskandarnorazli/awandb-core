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

package org.awandb.server

import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.awandb.core.sql.SQLHandler
import scala.jdk.CollectionConverters._

class AwanFlightProducer(allocator: BufferAllocator, location: Location) extends NoOpFlightProducer {

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    // 1. Define the Network Schema
    // We will package the SQL output into a single Arrow String column called "query_result"
    val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
    
    // 2. Create a ticket containing the SQL command
    val ticket = new Ticket(descriptor.getCommand)
    val endpoint = new FlightEndpoint(ticket, location)
    
    new FlightInfo(schema, descriptor, List(endpoint).asJava, -1, -1)
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    try {
      // 1. Read the SQL command from the Python Client's ticket
      val sql = new String(ticket.getBytes, "UTF-8")
      println(s"[Network] Executing Incoming SQL: $sql")

      // 2. Execute via the Native Volcano Pipeline
      // [FIX] Capture the structured SQLResult object
      val result = SQLHandler.execute(sql)

      // 3. Package the native result into a zero-copy Apache Arrow Vector
      val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
      val root = VectorSchemaRoot.create(schema, allocator)
      val resultVector = root.getVector("query_result").asInstanceOf[VarCharVector]

      resultVector.allocateNew()
      
      // [FIX] Extract the .message string from the result before converting to bytes
      resultVector.setSafe(0, result.message.getBytes("UTF-8"))
      
      resultVector.setValueCount(1)
      root.setRowCount(1)

      // 4. Stream it back to the client!
      listener.start(root)
      listener.putNext()
      root.close()
      listener.completed()

    } catch {
      case e: Exception =>
        println(s"[Network] Error executing query: ${e.getMessage}")
        listener.error(e)
    }
  }
}