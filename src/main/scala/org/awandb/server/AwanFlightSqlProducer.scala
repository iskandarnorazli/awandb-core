package org.awandb.server

import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.awandb.core.sql.SQLHandler
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets
import org.awandb.core.engine.EngineManager

class AwanFlightSqlProducer(allocator: BufferAllocator, location: Location, val targetTable: org.awandb.core.engine.AwanTable) extends FlightSqlProducer {

  // ========================================================================
  // 1. EXECUTE QUERY (SELECT) - The Read Pipeline
  // ========================================================================
  
  override def getFlightInfoStatement(command: CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
    
    // Pack a TicketStatementQuery into the Ticket (Required by Arrow Spec)
    val ticketMsg = TicketStatementQuery.newBuilder()
      .setStatementHandle(com.google.protobuf.ByteString.copyFromUtf8(command.getQuery))
      .build()
      
    val ticket = new Ticket(com.google.protobuf.Any.pack(ticketMsg).toByteArray)
    new FlightInfo(schema, descriptor, List(new FlightEndpoint(ticket, location)).asJava, -1, -1)
  }

  override def getSchemaStatement(command: CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = {
    val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
    new SchemaResult(schema)
  }

  override def getStreamStatement(ticket: TicketStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    var root: VectorSchemaRoot = null
    
    try {
      val sql = ticket.getStatementHandle.toStringUtf8
      println(s"[Network] 🟢 Executing SELECT: $sql")

      val resultString = SQLHandler.execute(sql)
      
      // Convert silent SQL strings into hard network exceptions
      if (resultString.startsWith("Error:") || resultString.startsWith("SQL Error:")) {
        throw new IllegalArgumentException(resultString)
      }

      val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
      root = VectorSchemaRoot.create(schema, allocator)
      val resultVector = root.getVector("query_result").asInstanceOf[VarCharVector]

      resultVector.allocateNew()
      resultVector.setSafe(0, resultString.getBytes("UTF-8"))
      resultVector.setValueCount(1)
      root.setRowCount(1)

      listener.start(root)
      listener.putNext()
      listener.completed()

    } catch {
      case e: net.sf.jsqlparser.parser.ParseException =>
        println(s"[Network] 🔴 SQL Parse Error: ${e.getMessage}")
        listener.error(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage).toRuntimeException)
      case e: Exception =>
        println(s"[Network] 🔴 Engine Crash: ${e.getMessage}")
        listener.error(CallStatus.INTERNAL.withDescription(e.getMessage).toRuntimeException)
    } finally {
      if (root != null) root.close()
    }
  }

  // ========================================================================
  // 2. EXECUTE UPDATE (INSERT / UPDATE / DELETE) - The Write Pipeline
  // ========================================================================
  
  override def acceptPutStatement(command: CommandStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, listener: FlightProducer.StreamListener[PutResult]): Runnable = {
    new Runnable {
      override def run(): Unit = {
        try {
          val sql = command.getQuery
          println(s"[Network] 🟡 Executing MUTATION: $sql")
          
          val resultString = SQLHandler.execute(sql)
          
          if (resultString.startsWith("Error:") || resultString.startsWith("SQL Error:")) {
            throw new IllegalArgumentException(resultString)
          }

          val updateResult = DoPutUpdateResult.newBuilder().setRecordCount(1L).build()
          val resultBytes = updateResult.toByteArray
          
          val arrowBuf = allocator.buffer(resultBytes.length)
          try {
            arrowBuf.writeBytes(resultBytes)
            val putResult = PutResult.metadata(arrowBuf)
            
            listener.onNext(putResult)
            listener.onCompleted()
          } finally {
            arrowBuf.close() 
          }
          
        } catch {
          case e: Exception =>
            println(s"[Network] 🔴 Engine Crash: ${e.getMessage}")
            listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage).toRuntimeException)
        }
      }
    }
  }

  // =========================================================================
  // CUSTOM RPC ENDPOINTS (Bypassing SQL Parser for Native JSON Ingestion)
  // =========================================================================
  override def doAction(
      context: FlightProducer.CallContext, 
      action: Action, 
      listener: FlightProducer.StreamListener[org.apache.arrow.flight.Result]
  ): Unit = {
    
    val actionType = action.getType
    
    if (actionType.startsWith("IngestJson:")) {
      try {
        // Parse the raw bytes directly into the Shredder using the injected table
        val jsonPayload = new String(action.getBody, StandardCharsets.UTF_8)
        val insertedRows = org.awandb.core.ingest.JsonShredder.ingest(jsonPayload, targetTable)
        
        // Return success response to the client
        val resultBytes = s"""{"inserted": $insertedRows}""".getBytes(StandardCharsets.UTF_8)
        listener.onNext(new org.apache.arrow.flight.Result(resultBytes))
        listener.onCompleted()
        
      } catch {
        case e: Exception => 
          listener.onError(
            CallStatus.INTERNAL
              .withDescription(s"JSON Ingestion failed: ${e.getMessage}")
              .withCause(e)
              .toRuntimeException
          )
      }
    } else {
      // Vital: Fallback to standard Arrow Flight SQL actions (like query cancellation)
      super.doAction(context, action, listener)
    }
  }

  // ========================================================================
  // 3. BASE INTERFACE REQUIREMENTS
  // ========================================================================
  
  override def close(): Unit = {}
  
  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = throw CallStatus.UNIMPLEMENTED.toRuntimeException

  // --- Flight Info Endpoints ---
  private def getEmptyInfo(descriptor: FlightDescriptor): FlightInfo = {
    val schema = new Schema(java.util.Collections.emptyList())
    new FlightInfo(schema, descriptor, java.util.Collections.emptyList(), 0, 0)
  }

  override def getFlightInfoCatalogs(command: CommandGetCatalogs, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoSchemas(command: CommandGetDbSchemas, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoTables(command: CommandGetTables, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoTableTypes(command: CommandGetTableTypes, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoSqlInfo(command: CommandGetSqlInfo, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoTypeInfo(command: CommandGetXdbcTypeInfo, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = getEmptyInfo(descriptor)
  override def getFlightInfoPreparedStatement(command: CommandPreparedStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def getFlightInfoPrimaryKeys(command: CommandGetPrimaryKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def getFlightInfoExportedKeys(command: CommandGetExportedKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def getFlightInfoImportedKeys(command: CommandGetImportedKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def getFlightInfoCrossReference(command: CommandGetCrossReference, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = throw CallStatus.UNIMPLEMENTED.toRuntimeException

  // --- Data Stream Endpoints ---
  override def getStreamCatalogs(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamSchemas(command: CommandGetDbSchemas, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamTables(command: CommandGetTables, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamTableTypes(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamPrimaryKeys(command: CommandGetPrimaryKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamExportedKeys(command: CommandGetExportedKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamImportedKeys(command: CommandGetImportedKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamCrossReference(command: CommandGetCrossReference, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamSqlInfo(command: CommandGetSqlInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()
  override def getStreamTypeInfo(command: CommandGetXdbcTypeInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = listener.completed()

  // --- Prepared Statement Endpoints ---
  override def createPreparedStatement(request: ActionCreatePreparedStatementRequest, context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[org.apache.arrow.flight.Result]): Unit = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def closePreparedStatement(request: ActionClosePreparedStatementRequest, context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[org.apache.arrow.flight.Result]): Unit = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def getStreamPreparedStatement(command: CommandPreparedStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def acceptPutPreparedStatementUpdate(command: CommandPreparedStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, listener: FlightProducer.StreamListener[PutResult]): Runnable = throw CallStatus.UNIMPLEMENTED.toRuntimeException
  override def acceptPutPreparedStatementQuery(command: CommandPreparedStatementQuery, context: FlightProducer.CallContext, flightStream: FlightStream, listener: FlightProducer.StreamListener[PutResult]): Runnable = throw CallStatus.UNIMPLEMENTED.toRuntimeException
}