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
import org.awandb.server.middleware.FormatMiddleware
import org.awandb.core.jni.NativeBridge // <-- Added for Zero-Copy

class AwanFlightSqlProducer(allocator: BufferAllocator, location: Location) extends FlightSqlProducer {

  // ========================================================================
  // 1. EXECUTE QUERY (SELECT) - The Read Pipeline
  // ========================================================================
  
  override def getFlightInfoStatement(command: CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val sql = command.getQuery
    
    // 1. Intercept the requested format
    val mw = context.getMiddleware(FormatMiddleware.Key)
    val format = if (mw != null) mw.asInstanceOf[FormatMiddleware].getFormat() else "string"

    // 2. 🚀 PREDICT THE SCHEMA USING THE INFERENCER!
    val schemaFields = if (format == "arrow") {
      val inferred = SQLHandler.inferSchema(sql)
      
      if (inferred.isEmpty) {
         // DDL Fallback: Empty arrays still need the standard query_result confirmation
         List(Field.nullable("query_result", new ArrowType.Utf8())).asJava
      } else {
         // Map our inferred AwanDB types into pure Arrow Flight Schema Fields
         inferred.map { case (colName, colType) =>
           val arrowType = colType match {
             case "INT" => new ArrowType.Int(32, true)
             case "STRING" => new ArrowType.Utf8()
             case _ => new ArrowType.Utf8() // Fallback
           }
           Field.nullable(colName, arrowType)
         }.toList.asJava
      }
    } else {
      // Legacy path
      List(Field.nullable("query_result", new ArrowType.Utf8())).asJava
    }

    val schema = new Schema(schemaFields)
    
    // 3. Pack a TicketStatementQuery into the Ticket (Required by Arrow Spec)
    val ticketMsg = TicketStatementQuery.newBuilder()
      .setStatementHandle(com.google.protobuf.ByteString.copyFromUtf8(sql))
      .build()
      
    val ticket = new Ticket(com.google.protobuf.Any.pack(ticketMsg).toByteArray)
    
    // 4. Return the Promise!
    new FlightInfo(schema, descriptor, List(new FlightEndpoint(ticket, location)).asJava, -1, -1)
  }

  override def getSchemaStatement(command: CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = {
    val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
    new SchemaResult(schema)
  }

  override def getStreamStatement(ticket: TicketStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    var root: VectorSchemaRoot = null
    
    // 1. Generate a Unique ID to track this query's C++ memory arena
    val queryId = java.util.UUID.randomUUID().toString 
    
    try {
      val sql = ticket.getStatementHandle.toStringUtf8
      
      // 2. Initialize the Memory Arena in C++
      NativeBridge.initQueryContext(queryId) 
      
      // START PROFILER
      AwanDBInternalProfiler.start()
      println(s"[Network] 🟢 Executing SELECT [$queryId]: $sql")

      // PHASE 1 (Prep & Parse Boundary)
      AwanDBInternalProfiler.stampPhase1()

      // [FIX 3] Receive the structured SQLResult
      // Note: Make sure to update your SQLHandler to accept and pass the queryId down to the operators if needed!
      val result = SQLHandler.execute(sql) 
      
      // PHASE 2 (Engine Execution Boundary)
      AwanDBInternalProfiler.stampPhase2()
      
      // Hard failure on actual boolean error
      if (result.isError) {
        throw new IllegalArgumentException(result.message)
      }

      // 3. ABORT CHECK: If the Python client crashed or dropped during C++ execution, abort Arrow packing!
      if (listener.isCancelled) {
         println(s"[Network] 🟡 Client disconnected early. Aborting Arrow packaging for $queryId.")
         return // The 'finally' block will safely clean up the memory.
      }

      // 1. Intercept the Requested Format from Middleware
      val mw = context.getMiddleware(FormatMiddleware.Key)
      val format = if (mw != null) mw.asInstanceOf[FormatMiddleware].getFormat() else "string"

      if (result.affectedRows == 0L) {
        // [EMPTY PATH] Safely close stream without sending data payloads
        val emptySchema = if (format == "arrow") {
           if (result.schema.isEmpty) {
              // 🚀 SCHEMA FALLBACK FIX: Match the getFlightInfo promise for DDL queries!
              new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
           } else {
               // Standard typed schema for 0-row SELECTs (e.g., SELECT * WHERE 1=0)
               new Schema(result.schema.map { case (colName, colType) =>
                  val arrowType = colType match {
                    case "INT" => new ArrowType.Int(32, true)
                    case "STRING" => new ArrowType.Utf8()
                    case _ => new ArrowType.Utf8()
                  }
                  Field.nullable(colName, arrowType)
               }.toList.asJava)
           }
        } else {
           // Legacy String Payload Format
           new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
        }
        
        root = VectorSchemaRoot.create(emptySchema, allocator)
        root.setRowCount(0)
        
        // 4. SECOND ABORT CHECK
        if (!listener.isCancelled) {
          listener.start(root) // Sends the Schema only
          listener.completed()
        }
      } 
      else if (format == "arrow") {
        // ---------------------------------------------------------
        // [OLAP PATH] DYNAMIC COLUMNAR ARROW PAYLOAD
        // ---------------------------------------------------------
        val fields = result.schema.map { case (colName, colType) =>
          val arrowType = colType match {
            case "INT" => new ArrowType.Int(32, true)
            case "STRING" => new ArrowType.Utf8()
            case _ => new ArrowType.Utf8() // Fallback
          }
          Field.nullable(colName, arrowType)
        }.toList.asJava

        val schema = new Schema(fields)
        root = VectorSchemaRoot.create(schema, allocator)
        val rowCount = result.affectedRows.toInt
        root.setRowCount(rowCount)

        // Populate Arrow Vectors natively from the transposed arrays
        for (c <- result.schema.indices) {
            val vector = root.getVector(c)
            val colType = result.schema(c)._2
            val rawData = result.columnarData(c)

            colType match {
                case "INT" =>
                    val intVec = vector.asInstanceOf[IntVector]
                    intVec.allocateNew(rowCount)
                    
                    val arrowDataPtr = intVec.getDataBuffer.memoryAddress()
                    
                    // 🚀 TRUE ZERO-COPY UPGRADE
                    rawData match {
                      case ptr: java.lang.Long => 
                        NativeBridge.memcpy(ptr, arrowDataPtr, rowCount * 4L)
                      case arr: Array[Int] =>
                        NativeBridge.loadData(arrowDataPtr, arr)
                      case _ => 
                        throw new IllegalArgumentException(s"Unknown data type for INT column: ${rawData.getClass.getSimpleName}")
                    }
                    
                    // 🚀 O(1) FAST-PATH VALIDITY BUFFER 
                    val validityBytes = (rowCount + 7) / 8
                    val ones = Array.fill[Byte](validityBytes)((-1).toByte)
                    intVec.getValidityBuffer.setBytes(0, ones, 0, validityBytes)
                    
                    intVec.setValueCount(rowCount)
                    
                case "STRING" =>
                    val strVec = vector.asInstanceOf[VarCharVector]
                    strVec.allocateNew(rowCount)
                    val arr = rawData.asInstanceOf[Array[String]]
                    var i = 0
                    while (i < rowCount) {
                        val s = if (arr(i) == null) "" else arr(i)
                        strVec.setSafe(i, s.getBytes("UTF-8"))
                        i += 1
                    }
                    strVec.setValueCount(rowCount)
                case _ => // Handled via string fallback for now
            }
        }

        // 4. SECOND ABORT CHECK
        if (!listener.isCancelled) {
          listener.start(root)
          listener.putNext()
          listener.completed()
        } else {
          println(s"[Network] 🟡 Client dropped right before stream transfer for $queryId.")
        }
      } 
      else {
        // ---------------------------------------------------------
        // [OLTP PATH] LEGACY STRING PAYLOAD
        // ---------------------------------------------------------
        val schema = new Schema(List(Field.nullable("query_result", new ArrowType.Utf8())).asJava)
        root = VectorSchemaRoot.create(schema, allocator)
        
        val resultVector = root.getVector("query_result").asInstanceOf[VarCharVector]
        resultVector.allocateNew()
        resultVector.setSafe(0, result.message.getBytes("UTF-8"))
        resultVector.setValueCount(1)
        root.setRowCount(1)

        // 4. SECOND ABORT CHECK
        if (!listener.isCancelled) {
          listener.start(root)
          listener.putNext()
          listener.completed()
        } else {
          println(s"[Network] 🟡 Client dropped right before stream transfer for $queryId.")
        }
      }

      // 🚀 STOP PROFILER (Arrow Packing Complete)
      AwanDBInternalProfiler.finish()

    } catch {
      case e: net.sf.jsqlparser.parser.ParseException =>
        println(s"[Network] 🔴 SQL Parse Error: ${e.getMessage}")
        listener.error(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage).toRuntimeException)
      case e: Throwable =>
        println(s"[Network] 🔴 Engine Crash: ${e.getMessage}")
        listener.error(CallStatus.INTERNAL.withDescription(e.getMessage).toRuntimeException)
    } finally {
      if (root != null) root.close()
      
      // 5. THE FAILSAFE: Guarantee C++ memory teardown regardless of crashes, drops, or success!
      NativeBridge.destroyQueryContext(queryId)
      println(s"[Memory] 🧹 Reclaimed C++ Query Arena for: $queryId")
    }
  }

  // ========================================================================
  // 2. EXECUTE UPDATE (INSERT / UPDATE / DELETE) - The Write Pipeline
  // ========================================================================
  
  override def acceptPutStatement(command: CommandStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, listener: FlightProducer.StreamListener[PutResult]): Runnable = {
    new Runnable {
      override def run(): Unit = {
        // [CRITICAL FIX] Generate an arena ID
        val queryId = java.util.UUID.randomUUID().toString
        
        try {
          val sql = command.getQuery
          println(s"[Network] 🟢 Executing MUTATION: $sql")
          
          // [CRITICAL FIX] Bind this DML command to a safe C++ memory arena
          NativeBridge.initQueryContext(queryId)
          
          val result = SQLHandler.execute(sql)
          if (result.isError) {
            throw new IllegalArgumentException(result.message)
          }

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
          case e: Throwable =>
            println(s"[Network] 🔴 Engine Crash: ${e.getMessage}")
            listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage).toRuntimeException)
        } finally {
          // [CRITICAL FIX] THE FAILSAFE: Guarantee C++ memory teardown for DML queries
          NativeBridge.destroyQueryContext(queryId)
          println(s"[Memory] 🧹 Reclaimed C++ Query Arena for DML: $queryId")
        }
      }
    }
  }

  // ========================================================================
  // 3. BASE INTERFACE REQUIREMENTS
  // ========================================================================
  
  override def close(): Unit = {}
  
  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = throw CallStatus.UNIMPLEMENTED.toRuntimeException

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
    
    // The Routing Switch:
    if (descriptor.isCommand) {
      return super.acceptPut(context, flightStream, ackStream)
    }

    new Runnable {
      override def run(): Unit = {
        try {
          val tableName = descriptor.getPath.get(0).toLowerCase()
          val table = SQLHandler.tables.get(tableName)
          if (table == null) throw CallStatus.NOT_FOUND.withDescription(s"Table '$tableName' does not exist.").toRuntimeException

          var totalRowsIngested = 0L
          
          table.withEpoch {
            while (flightStream.next()) {
              val root = flightStream.getRoot
              val rowCount = root.getRowCount

              if (rowCount > 0) {
                // ---------------------------------------------------------
                // UNIVERSAL ZERO-COPY FAST-PATH DETECTOR
                // ---------------------------------------------------------
                var canBulkLoad = true
                val colCount = root.getFieldVectors.size()
                
                // FIX: Strict Schema Width Check!
                // If the Arrow payload is missing columns, force it into the Slow Path for padding.
                if (colCount != table.columns.size) {
                    canBulkLoad = false
                }
                
                val colNames = new Array[String](colCount)
                val colTypes = new Array[Int](colCount)
                val dataPtrs = new Array[Long](colCount)
                val offsetPtrs = new Array[Long](colCount)
                val sizes = new Array[Int](colCount)
                val dims = new Array[Int](colCount)
                
                for (colIdx <- 0 until colCount) {
                    val vector = root.getVector(colIdx)
                    val vectorName = vector.getName.toLowerCase
                    val resolvedName = if (table.columns.contains(vectorName)) vectorName else table.columnOrder(colIdx)
                    
                    colNames(colIdx) = resolvedName
                    val col = table.columns(resolvedName)
                    
                    if (vector.isInstanceOf[IntVector] && !col.isString && !col.isVector) {
                        colTypes(colIdx) = 0
                        dataPtrs(colIdx) = vector.asInstanceOf[IntVector].getDataBuffer.memoryAddress()
                        sizes(colIdx) = rowCount * 4
                    } 
                    else if (vector.isInstanceOf[VarCharVector] && col.isString) {
                        colTypes(colIdx) = 2
                        val strVec = vector.asInstanceOf[VarCharVector]
                        offsetPtrs(colIdx) = strVec.getOffsetBuffer.memoryAddress()
                        dataPtrs(colIdx) = strVec.getDataBuffer.memoryAddress()
                        
                        // Exact String Pool Calculation! 
                        // Arrow's offset buffer safely tracks the exact byte-length of all variable strings combined.
                        val totalStringBytes = strVec.getOffsetBuffer.getInt(rowCount * 4L)
                        sizes(colIdx) = (rowCount * 16) + totalStringBytes
                    }
                    else if (vector.isInstanceOf[org.apache.arrow.vector.complex.ListVector] && col.isVector) {
                        colTypes(colIdx) = 3
                        val listVec = vector.asInstanceOf[org.apache.arrow.vector.complex.ListVector]
                        val innerVec = listVec.getDataVector.asInstanceOf[org.apache.arrow.vector.Float4Vector]
                        
                        val totalFloats = innerVec.getValueCount
                        val dim = if (rowCount > 0) totalFloats / rowCount else 0
                        
                        dims(colIdx) = dim
                        dataPtrs(colIdx) = innerVec.getDataBuffer.memoryAddress()
                        sizes(colIdx) = totalFloats * 4 // 4 bytes per float
                    }
                    else {
                        canBulkLoad = false
                    }
                }

                if (canBulkLoad) {
                    // TRUE ZERO-COPY FOR ALL TYPES
                    table.bulkLoadFromArrowPointers(colNames, colTypes, dataPtrs, offsetPtrs, sizes, dims, rowCount)
                    totalRowsIngested += rowCount
                } else {
                    // 🐢🐢🐢 SLOW PATH: Legacy Mixed-Type Ingestion 🐢🐢🐢
                    val incomingCols = scala.collection.mutable.Set[String]()

                    // 1. Ingest provided columns
                    for (colIdx <- 0 until root.getFieldVectors.size()) {
                      val vector = root.getVector(colIdx)
                      
                      val vectorName = vector.getName.toLowerCase
                      val colName = if (table.columns.contains(vectorName)) {
                        vectorName
                      } else {
                        table.columnOrder(colIdx)
                      }

                      incomingCols.add(colName)

                      vector match {
                        case intVector: org.apache.arrow.vector.IntVector =>
                          if (table.columns(colName).isString || table.columns(colName).isVector) {
                            root.clear()
                            throw CallStatus.INVALID_ARGUMENT
                              .withDescription(s"Unsupported Arrow stream type. Found: IntVector on column '$colName'")
                              .toRuntimeException
                          }

                          val rawPointer = intVector.getDataBuffer.memoryAddress()
                          table.insertBatchFromPointer(colName, rawPointer, rowCount)
                        
                        // 🚀 NEW: Handle 64-bit Integers (Downcast to 32-bit for JVM Legacy Loop)
                        case bigIntVector: org.apache.arrow.vector.BigIntVector =>
                          if (table.columns(colName).isString || table.columns(colName).isVector) {
                            root.clear()
                            throw CallStatus.INVALID_ARGUMENT
                              .withDescription(s"Unsupported Arrow stream type. Found: BigIntVector on column '$colName'")
                              .toRuntimeException
                          }
                          
                          val arr = new Array[Int](rowCount)
                          var i = 0
                          while (i < rowCount) {
                            // Read 64-bit Long and downcast to 32-bit Int
                            arr(i) = bigIntVector.get(i).toInt 
                            i += 1
                          }
                          table.insertBatch(colName, arr)

                        case stringVector: org.apache.arrow.vector.VarCharVector =>
                          if (!table.columns(colName).isString) {
                            root.clear()
                            throw CallStatus.INVALID_ARGUMENT
                              .withDescription(s"Unsupported Arrow stream type. Found: VarCharVector on column '$colName'")
                              .toRuntimeException
                          }

                          var i = 0
                          while (i < rowCount) {
                            val bytes = stringVector.get(i)
                            val strVal = if (bytes == null) "" else new String(bytes, "UTF-8")
                            table.columns(colName).insert(strVal)
                            i += 1
                          }
                          
                        case listVector: org.apache.arrow.vector.complex.ListVector =>
                          if (!table.columns(colName).isVector) {
                            root.clear()
                            throw CallStatus.INVALID_ARGUMENT
                              .withDescription(s"Unsupported Arrow stream type. Found: ListVector on column '$colName'")
                              .toRuntimeException
                          }

                          listVector.getDataVector match {
                            case innerVector: org.apache.arrow.vector.Float4Vector =>
                              var i = 0
                              while (i < rowCount) {
                                if (listVector.isNull(i)) {
                                  table.columns(colName).insert(Array.empty[Float])
                                } else {
                                  val startIdx = listVector.getOffsetBuffer.getInt(i * 4L)
                                  val endIdx = listVector.getOffsetBuffer.getInt((i + 1) * 4L)
                                  val dim = endIdx - startIdx
                                  
                                  val floatArr = new Array[Float](dim)
                                  var j = 0
                                  while (j < dim) {
                                    floatArr(j) = innerVector.get(startIdx + j)
                                    j += 1
                                  }
                                  
                                  table.columns(colName).insert(floatArr)
                                }
                                i += 1
                              }
                            case _ =>
                              root.clear()
                              throw CallStatus.INVALID_ARGUMENT
                                .withDescription(s"Vector column '$colName' expects a List of Floats (Float4).")
                                .toRuntimeException
                          }
                          
                        case _ => 
                          root.clear()
                          throw CallStatus.INVALID_ARGUMENT.withDescription(s"Unsupported Arrow stream type. Found: ${vector.getClass.getSimpleName} on column '$colName'").toRuntimeException
                      }
                    }

                    // 2. Pad missing columns with default values to maintain row alignment
                    val missingCols = table.columnOrder.filterNot(incomingCols.contains)
                    for (colName <- missingCols) {
                       val col = table.columns(colName)
                       if (col.isVector) {
                          val emptyVec = Array.empty[Float]
                          var i = 0
                          while (i < rowCount) {
                             col.insert(emptyVec)
                             i += 1
                          }
                       } else if (col.isString) {
                          var i = 0
                          while (i < rowCount) {
                             col.insert("")
                             i += 1
                          }
                       } else {
                          val padData = new Array[Int](rowCount) 
                          table.insertBatch(colName, padData)
                       }
                    }

                    totalRowsIngested += rowCount 

                    // We ONLY call standard flush if we are using the Slow RAM Path!
                    // Bulk load flushes to disk natively.
                    if (totalRowsIngested % 1000000 == 0) {
                        table.flush()
                    }
                }
              }
            }
          } // End of withEpoch block

          ackStream.onNext(PutResult.empty())
          ackStream.onCompleted()

        } catch {
          case fre: FlightRuntimeException => ackStream.onError(fre)
          case e: Throwable => ackStream.onError(CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage).toRuntimeException)
        } finally {
          try { flightStream.close() } catch { case _: Exception => }
        }
      }
    }
  }
}