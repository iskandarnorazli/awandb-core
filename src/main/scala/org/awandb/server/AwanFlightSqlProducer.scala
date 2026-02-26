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
import org.apache.arrow.vector.IntVector
import org.apache.arrow.flight.{CallStatus, FlightRuntimeException, FlightStream, PutResult}

class AwanFlightSqlProducer(allocator: BufferAllocator, location: Location) extends FlightSqlProducer {

  // ========================================================================
  // 1. EXECUTE QUERY (SELECT) - The Read Pipeline
  // ========================================================================
  
  override def getFlightInfoStatement(command: CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
    
    // [FIX] Pack a TicketStatementQuery into the Ticket (Required by Arrow Spec)
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

      // [FIX 3] Receive the structured SQLResult
      val result = SQLHandler.execute(sql)
      
      // [FIX 3] Hard failure on actual boolean error
      if (result.isError) {
        throw new IllegalArgumentException(result.message)
      }

      val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
      root = VectorSchemaRoot.create(schema, allocator)
      
      // [BUG FIX 1] Check affectedRows directly to see if result set is empty
      if (result.affectedRows == 0L) {
        // [EMPTY PATH] Safely close stream without sending data payloads
        root.setRowCount(0)
        listener.start(root) // Sends the Schema only
        listener.completed()
      } else {
        // [DATA PATH] Send the row payload
        val resultVector = root.getVector("query_result").asInstanceOf[VarCharVector]
        resultVector.allocateNew()
        resultVector.setSafe(0, result.message.getBytes("UTF-8"))
        resultVector.setValueCount(1)
        root.setRowCount(1)

        listener.start(root)
        listener.putNext()
        listener.completed()
      }

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
          println(s"[Network] 🟢 Executing MUTATION: $sql")
          
          // [FIX 3] Receive the structured SQLResult
          val result = SQLHandler.execute(sql)
          
          // [FIX 3] Hard failure on actual boolean error, no brittle string matching
          if (result.isError) {
            throw new IllegalArgumentException(result.message)
          }

          // [FIX 2] Inject the ACTUAL affected row count back to the ORM!
          val updateResult = DoPutUpdateResult.newBuilder().setRecordCount(result.affectedRows).build()
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

  // ========================================================================
  // 4. RAW BINARY INGESTION (DoPut) - Bypassing SQL
  // ========================================================================
  override def acceptPut(
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]
  ): Runnable = {
    
    val descriptor = flightStream.getDescriptor
    
    // [FIX] The Routing Switch:
    // Flight SQL sends mutations as Commands (Protobuf Any messages).
    // If it's a command, hand it back to the parent class so it can unpack 
    // the protobuf and route it to our `acceptPutStatement` method!
    if (descriptor.isCommand) {
      return super.acceptPut(context, flightStream, ackStream)
    }

    // Otherwise, it's a Path descriptor, which means it's our raw binary ingestion stream!
    new Runnable {
      override def run(): Unit = {
        try {
          val tableName = descriptor.getPath.get(0).toLowerCase()

          val table = SQLHandler.tables.get(tableName)
          if (table == null) throw CallStatus.NOT_FOUND.withDescription(s"Table '$tableName' does not exist.").toRuntimeException

          var totalRowsIngested = 0L
          while (flightStream.next()) {
            val root = flightStream.getRoot
            val rowCount = root.getRowCount

            if (rowCount > 0) {
              // [FIX] Iterate over ALL columns in the Arrow stream
              for (colIdx <- 0 until root.getFieldVectors.size()) {
                val vector = root.getVector(colIdx)
                
                // Map Arrow vector name to DB column name, fallback to index
                val vectorName = vector.getName.toLowerCase
                val colName = if (table.columns.contains(vectorName)) {
                  vectorName
                } else {
                  table.columnOrder(colIdx)
                }

                vector match {
                  case intVector: org.apache.arrow.vector.IntVector =>
                    val batchData = new Array[Int](rowCount)
                    var i = 0
                    while (i < rowCount) { 
                      batchData(i) = intVector.get(i)
                      i += 1 
                    }
                    
                    // Routes through the table so the Primary Index gets updated
                    table.insertBatch(colName, batchData)
                    
                  case _ => 
                    root.clear()
                    throw CallStatus.INVALID_ARGUMENT.withDescription(s"Only IntVector streams are currently supported. Found: ${vector.getClass.getSimpleName} on column '$colName'").toRuntimeException
                }
              }
              // Add row count only once per batch, not per column
              totalRowsIngested += rowCount 
            }
          }
          ackStream.onNext(PutResult.empty())
          ackStream.onCompleted()

        } catch {
          case fre: FlightRuntimeException => ackStream.onError(fre)
          case e: Exception => ackStream.onError(CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage).toRuntimeException)
        } finally {
          try { flightStream.close() } catch { case _: Exception => }
        }
      }
    }
  }
}