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

package org.awandb.core.engine

import org.awandb.core.query._
import org.awandb.core.jni.NativeBridge

object QueryExecutor {
  
  /**
   * Synchronous execution of a physical operator plan (DAG).
   * Automatically initializes and reclaims the C++ Query Context safely.
   * * @param queryId Unique identifier for the query memory arena
   * @param rootOp The root of the operator tree (e.g., Sort or Agg)
   * @return Number of rows in the final result
   */
  def execute(queryId: String, rootOp: Operator): Long = {
    var rowsProcessed = 0L
    
    // 1. Initialize C++ Query Context
    NativeBridge.initQueryContext(queryId)
    
    try {
      // 2. Initialize the Operator Tree (Recursive)
      rootOp.open()
      
      try {
        // 3. Consume Batches (Volcano Loop)
        var batch = rootOp.next()
        while (batch != null) {
          rowsProcessed += batch.count
          // Server-side internal executions (e.g., INSERT INTO ... SELECT)
          batch = rootOp.next()
        }
      } finally {
        // Ensure operators release internal Scala/JNI state
        rootOp.close()
      }
      
    } finally {
      // 4. Guarantee complete native memory wipe on success OR failure!
      NativeBridge.destroyQueryContext(queryId)
      println(s"[QueryExecutor] 🧹 Reclaimed C++ Query Arena for: $queryId")
    }
    
    rowsProcessed
  }

  /**
   * [Phase 6 Hook] Streaming execution of a physical operator plan (DAG).
   * Wraps the Volcano pipeline in a safe Iterator that destroys the C++ 
   * memory arena the moment the query stream is exhausted or dropped.
   * * @param queryId Unique identifier for the query memory arena
   * @param rootOp The root of the operator tree
   * @return An iterator of execution batches
   */
  def executeStream(queryId: String, rootOp: Operator): Iterator[VectorBatch] = {
    // 1. Initialize C++ Query Context
    NativeBridge.initQueryContext(queryId)
    rootOp.open()
    
    var isClosed = false

    new Iterator[VectorBatch] {
      private var nextBatch: VectorBatch = _
      private var fetched = false

      private def fetchNext(): Unit = {
        if (!fetched && !isClosed) {
          // You may need to cast this if Operator.next() returns a generic type
          nextBatch = rootOp.next().asInstanceOf[VectorBatch] 
          fetched = true
          
          if (nextBatch == null) {
             cleanup() // Normal Exhaustion!
          }
        }
      }

      private def cleanup(): Unit = {
        if (!isClosed) {
          try { 
            rootOp.close() 
          } catch { 
            case e: Exception => println(s"Error closing operator: ${e.getMessage}") 
          } finally {
            // Memory is wiped safely upon exhaustion!
            NativeBridge.destroyQueryContext(queryId)
            println(s"[QueryExecutor] 🧹 Stream Exhausted. Reclaimed arena: $queryId")
            isClosed = true
          }
        }
      }

      override def hasNext: Boolean = {
        fetchNext()
        nextBatch != null
      }

      override def next(): VectorBatch = {
        if (!hasNext) throw new NoSuchElementException("Query exhausted or closed")
        val b = nextBatch
        fetched = false // Reset to fetch next on subsequent hasNext() call
        b
      }
    }
  }
}