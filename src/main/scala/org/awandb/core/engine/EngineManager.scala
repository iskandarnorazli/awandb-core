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

package org.awandb.core.engine

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Promise, Future}

// ==================================================================================
// [OPEN CORE ARCHITECTURE] GOVERNANCE INTERFACE
// ==================================================================================

trait EngineGovernor {
  def canAcceptInsert(tenantId: String): Boolean
  def recordUsage(ops: Int, bytes: Long): Unit
}

object NoOpGovernor extends EngineGovernor {
  override def canAcceptInsert(tenantId: String): Boolean = true
  override def recordUsage(ops: Int, bytes: Long): Unit = {}
}

// ==================================================================================
// ENGINE COMMAND MESSAGES (Updated for Mixed Types)
// ==================================================================================

sealed trait EngineCommand
case class InsertIntCommand(value: Int) extends EngineCommand
case class InsertStringCommand(value: String) extends EngineCommand
case class FlushCommand() extends EngineCommand
case object StopCommand extends EngineCommand
// Query Command (Supports Polymorphic Search)
case class QueryCommand(colName: String, value: Any, promise: Promise[Int]) extends EngineCommand

// ==================================================================================
// ENGINE MANAGER
// Focuses exclusively on Command Fusion (Batching) for high throughput.
// ==================================================================================

class EngineManager(table: AwanTable, governor: EngineGovernor = NoOpGovernor) extends Thread {
  
  // 1. High-Speed Command Queue (Inserts, Queries)
  private val queue = new LinkedBlockingQueue[EngineCommand]()
  private val running = new AtomicBoolean(false)
  
  // Metrics
  val processedCount = new AtomicLong(0)

  // ----------------------------------------------------------------
  // LIFECYCLE
  // ----------------------------------------------------------------

  override def start(): Unit = {
    super.start()
    println("[EngineManager] Started Event Loop (Fusion Engine)")
  }

  def stopEngine(): Unit = {
    queue.offer(StopCommand)
  }

  def joinThread(): Unit = {
    try {
      this.join()
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }

  // ----------------------------------------------------------------
  // EVENT LOOP (High Priority)
  // ----------------------------------------------------------------

  override def run(): Unit = {
    running.set(true)

    try {
      while (running.get()) {
        val cmd = queue.poll(1, TimeUnit.SECONDS)
        
        if (cmd != null) {
          try {
            cmd match {
              case InsertIntCommand(value) => handleBatchInsertInt(value)
              case InsertStringCommand(value) => handleBatchInsertString(value)
              
              case FlushCommand() => 
                // Synchronous Flush (Data + Index)
                table.flush() 
              
              case StopCommand => 
                println("[EngineManager] Stopping...")
                running.set(false)
              
              case q: QueryCommand => handleBatchQuery(q)
            }
          } catch {
            case e: Exception =>
              println(s"[EngineManager] ERROR processing command: ${e.getMessage}")
              if (cmd.isInstanceOf[QueryCommand]) {
                cmd.asInstanceOf[QueryCommand].promise.failure(e)
              }
          }
        }
      }
    } catch {
      case e: Throwable =>
        println(s"[EngineManager] FATAL CRASH: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println(s"[EngineManager] Thread Exited. Total Ops Processed: ${processedCount.get()}")
    }
  }

  // ----------------------------------------------------------------
  // BATCH HANDLERS (Fusion Logic)
  // ----------------------------------------------------------------

  private def handleBatchInsertInt(firstVal: Int): Unit = {
    if (!governor.canAcceptInsert("default_tenant")) return 

    // Assume default integer column "val" for simple single-column tests
    // In a real app, the command would carry the column name
    table.insert("val", firstVal)
    processedCount.incrementAndGet()
    governor.recordUsage(1, 4)
    
    // [WRITE FUSION] Drain Integers
    var ops = 0
    while (!queue.isEmpty && ops < 1000) {
      val nextCmd = queue.peek()
      if (nextCmd != null && nextCmd.isInstanceOf[InsertIntCommand]) {
        val insertCmd = queue.poll().asInstanceOf[InsertIntCommand]
        
        if (governor.canAcceptInsert("default_tenant")) {
            table.insert("val", insertCmd.value)
            processedCount.incrementAndGet()
            governor.recordUsage(1, 4)
        }
        ops += 1
      } else {
        return // Break batch on type switch
      }
    }
  }

  private def handleBatchInsertString(firstVal: String): Unit = {
    if (!governor.canAcceptInsert("default_tenant")) return 

    // Assume default string column "username" for tests
    table.insert("username", firstVal)
    processedCount.incrementAndGet()
    governor.recordUsage(1, firstVal.length)
    
    // [WRITE FUSION] Drain Strings
    var ops = 0
    while (!queue.isEmpty && ops < 1000) {
      val nextCmd = queue.peek()
      if (nextCmd != null && nextCmd.isInstanceOf[InsertStringCommand]) {
        val insertCmd = queue.poll().asInstanceOf[InsertStringCommand]
        val s = insertCmd.value
        
        if (governor.canAcceptInsert("default_tenant")) {
            table.insert("username", s)
            processedCount.incrementAndGet()
            governor.recordUsage(1, s.length)
        }
        ops += 1
      } else {
        return // Break batch
      }
    }
  }

  private def handleBatchQuery(firstCmd: QueryCommand): Unit = {
      // For now, we only fuse Integer Queries on the same column.
      // Complex polymorphic fusion is future work.
      if (!firstCmd.value.isInstanceOf[Int]) {
          val res = table.query(firstCmd.colName, firstCmd.value)
          firstCmd.promise.success(res)
          return
      }

      val batch = new ArrayBuffer[QueryCommand]()
      batch.append(firstCmd)
      
      // [QUERY FUSION] Fuse up to 100 queries
      var ops = 0
      while (!queue.isEmpty && ops < 100) { 
        val next = queue.peek()
        if (next != null && next.isInstanceOf[QueryCommand]) {
           val q = next.asInstanceOf[QueryCommand]
           // Only fuse if same column and type (Int)
           if (q.colName == firstCmd.colName && q.value.isInstanceOf[Int]) {
               batch.append(queue.poll().asInstanceOf[QueryCommand])
               ops += 1
           } else {
               processQueryBatch(batch)
               return
           }
        } else {
           processQueryBatch(batch)
           return
        }
      }
      processQueryBatch(batch)
  }

  private def processQueryBatch(batch: ArrayBuffer[QueryCommand]): Unit = {
      // Map Any -> Int for the shared scan
      val thresholds = batch.map(_.value.asInstanceOf[Int]).toArray
      
      // Shared Scan: 1 Pass for N queries
      val results = table.queryShared(thresholds)
      
      for (i <- batch.indices) {
        batch(i).promise.success(results(i))
      }
  }

  // ----------------------------------------------------------------
  // PUBLIC API
  // ----------------------------------------------------------------

  def submitInsert(value: Int): Unit = queue.offer(InsertIntCommand(value))
  def submitInsert(value: String): Unit = queue.offer(InsertStringCommand(value))
  
  def submitFlush(): Unit = queue.offer(FlushCommand())
  
  // Polymorphic Query Submission
  def submitQuery(colName: String, value: Any): Future[Int] = {
    val p = Promise[Int]()
    queue.offer(QueryCommand(colName, value, p))
    p.future
  }
}