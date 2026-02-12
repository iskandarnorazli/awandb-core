package org.awandb.server

import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener // [CRITICAL FIX] Explicit import
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.awandb.core.engine.AwanTable
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.jdk.CollectionConverters._

class AwanFlightProducer(db: AwanTable, allocator: BufferAllocator) extends NoOpFlightProducer {

  // 1. GetFlightInfo: "Planning" phase. 
  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val schema = getSchemaForTable()
    // Ensure the ticket handles the path correctly
    val path = if (descriptor.getPath.isEmpty) "default" else descriptor.getPath.get(0)
    val ticket = new Ticket(path.getBytes(StandardCharsets.UTF_8))
    
    val location = new Location("grpc://0.0.0.0:3000")
    val endpoint = new FlightEndpoint(ticket, location)
    
    new FlightInfo(schema, descriptor, Collections.singletonList(endpoint), -1, -1)
  }

  // 2. GetStream: "Execution" phase.
  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
    val tableName = new String(ticket.getBytes, StandardCharsets.UTF_8)
    println(s"[Server] Streaming data for table: $tableName")

    val root = VectorSchemaRoot.create(getSchemaForTable(), allocator)
    listener.start(root)

    try {
      val BATCH_SIZE = 1024
      val idVector = root.getVector("id").asInstanceOf[IntVector]
      val valVector = root.getVector("val").asInstanceOf[VarCharVector]

      var offset = 0
      // Stream 5 batches of 1024 rows
      for (batchIdx <- 0 until 5) {
        idVector.allocateNew()
        valVector.allocateNew()

        for (i <- 0 until BATCH_SIZE) {
          idVector.set(i, offset + i)
          valVector.setSafe(i, s"row_${offset + i}".getBytes(StandardCharsets.UTF_8))
        }

        idVector.setValueCount(BATCH_SIZE)
        valVector.setValueCount(BATCH_SIZE)
        root.setRowCount(BATCH_SIZE)

        listener.putNext() // Flush batch to network
        offset += BATCH_SIZE
      }
      
      listener.completed()
    } catch {
      case e: Exception => 
        e.printStackTrace()
        listener.error(e)
    } finally {
      root.close()
    }
  }

  // Define the Arrow Schema (Maps to AwanDB Columns)
  private def getSchemaForTable(): Schema = {
    val fields = new java.util.ArrayList[Field]()
    fields.add(Field.nullable("id", new ArrowType.Int(32, true)))
    fields.add(Field.nullable("val", new ArrowType.Utf8()))
    new Schema(fields)
  }
}