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
// ENGINE COMMAND MESSAGES
// ==================================================================================

sealed trait EngineCommand
case class InsertCommand(value: Int) extends EngineCommand
case class FlushCommand() extends EngineCommand
case object StopCommand extends EngineCommand
case class QueryCommand(threshold: Int, promise: Promise[Int]) extends EngineCommand

// ==================================================================================
// ENGINE MANAGER
// Focuses exclusively on Command Fusion (Batching) for high throughput.
// Background Indexing has been removed in favor of Fast Synchronous Writes.
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
    // Stop Event Loop gracefully
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
              case InsertCommand(value) => handleBatchInsert(value)
              case FlushCommand() => 
                // Synchronous Flush (Data + Index)
                // Fast enough now thanks to Software Prefetching
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

  private def handleBatchInsert(firstVal: Int): Unit = {
    if (!governor.canAcceptInsert("default_tenant")) return 

    table.insert(firstVal)
    processedCount.incrementAndGet()
    governor.recordUsage(1, 4)
    
    // [WRITE FUSION]
    // Drain up to 1000 items from queue to amortize locking/overhead
    var ops = 0
    while (!queue.isEmpty && ops < 1000) {
      val nextCmd = queue.peek()
      if (nextCmd != null && nextCmd.isInstanceOf[InsertCommand]) {
        val insertCmd = queue.poll().asInstanceOf[InsertCommand]
        
        if (governor.canAcceptInsert("default_tenant")) {
            table.insert(insertCmd.value)
            processedCount.incrementAndGet()
            governor.recordUsage(1, 4)
        }
        ops += 1
      } else {
        return // Break batch on different command type to preserve ordering
      }
    }
  }

  private def handleBatchQuery(firstCmd: QueryCommand): Unit = {
      val batch = new ArrayBuffer[QueryCommand]()
      batch.append(firstCmd)
      
      // [QUERY FUSION]
      // Fuse up to 100 queries into a single Scan Pass
      var ops = 0
      while (!queue.isEmpty && ops < 100) { 
        val next = queue.peek()
        if (next != null && next.isInstanceOf[QueryCommand]) {
           batch.append(queue.poll().asInstanceOf[QueryCommand])
           ops += 1
        } else {
           processQueryBatch(batch)
           return
        }
      }
      processQueryBatch(batch)
  }

  private def processQueryBatch(batch: ArrayBuffer[QueryCommand]): Unit = {
      val thresholds = batch.map(_.threshold).toArray
      // Shared Scan: 1 Pass for N queries (O(1) cost per added query)
      val results = table.queryShared(thresholds)
      for (i <- batch.indices) {
        batch(i).promise.success(results(i))
      }
  }

  // ----------------------------------------------------------------
  // PUBLIC API
  // ----------------------------------------------------------------

  def submitInsert(value: Int): Unit = queue.offer(InsertCommand(value))
  def submitFlush(): Unit = queue.offer(FlushCommand())
  
  def submitQuery(threshold: Int): Future[Int] = {
    val p = Promise[Int]()
    queue.offer(QueryCommand(threshold, p))
    p.future
  }
}