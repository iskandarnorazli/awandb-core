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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Promise, Future}

// ==================================================================================
// [OPEN CORE ARCHITECTURE] GOVERNANCE INTERFACE
// This allows the Enterprise Edition to inject logic for:
// 1. Overflow Guard (Rate Limiting)
// 2. Usage Metering (Billing)
// ==================================================================================

trait EngineGovernor {
  /**
   * Returns true if the system is healthy enough to accept a write.
   * Enterprise Implementation: Checks RAM usage, TPS quotas, or License limits.
   */
  def canAcceptInsert(tenantId: String): Boolean

  /**
   * Logs usage metrics for billing or internal cost accounting.
   * Enterprise Implementation: Aggregates stats to a high-speed ring buffer.
   */
  def recordUsage(ops: Int, bytes: Long): Unit
}

/**
 * [OSS DEFAULT] No-Op Governor.
 * Always returns true (infinite throughput), records nothing.
 */
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
// Now accepts an injected Governor for Enterprise functionality.
// ==================================================================================

class EngineManager(table: AwanTable, governor: EngineGovernor = NoOpGovernor) extends Thread {
  
  private val queue = new LinkedBlockingQueue[EngineCommand]()
  private val running = new AtomicBoolean(false)
  
  // DEBUGGING: Atomic counter to track what the Engine ACTUALLY processed
  val processedCount = new AtomicLong(0)

  override def run(): Unit = {
    running.set(true)
    println("[EngineManager] Started Event Loop")

    try {
      while (running.get()) {
        val cmd = queue.poll(1, TimeUnit.SECONDS)
        
        if (cmd != null) {
          try {
            cmd match {
              case InsertCommand(value) => handleBatchInsert(value)
              case FlushCommand() => 
                println("[EngineManager] Flushing to Disk...")
                table.flush()
              case StopCommand => 
                println("[EngineManager] Stopping...")
                running.set(false)
              case q: QueryCommand => handleBatchQuery(q)
            }
          } catch {
            case e: Exception =>
              println(s"[EngineManager] ERROR processing command: ${e.getMessage}")
              e.printStackTrace()
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

  private def handleBatchInsert(firstVal: Int): Unit = {
    // -------------------------------------------------------
    // [ENT HOOK] OVERFLOW GUARD (Phase 1, P1) - First Item
    // -------------------------------------------------------
    if (!governor.canAcceptInsert("default_tenant")) {
       // REJECT: Drop the write silently (or log it)
       // The Governor tracks the rejection statistic internally.
       return 
    }

    // Process first item
    table.insert(firstVal)
    processedCount.incrementAndGet()

    // -------------------------------------------------------
    // [ENT HOOK] METERING (Phase 4, P3)
    // -------------------------------------------------------
    governor.recordUsage(1, 4)
    
    // Batch process up to 1000 more pending items
    var ops = 0
    while (!queue.isEmpty && ops < 1000) {
      // Peek to see if it's an Insert (for optimization) or something else
      val nextCmd = queue.peek()
      
      if (nextCmd != null && nextCmd.isInstanceOf[InsertCommand]) {
        // It is an insert, pull it
        val insertCmd = queue.poll().asInstanceOf[InsertCommand]
        
        // [CRITICAL FIX] Must check Governor for EVERY item in the batch!
        if (governor.canAcceptInsert("default_tenant")) {
            table.insert(insertCmd.value)
            processedCount.incrementAndGet()
            governor.recordUsage(1, 4)
        } else {
            // Governor rejected this specific item in the batch.
            // We continue the loop to drain the queue, but we don't insert.
        }
        
        ops += 1
      } else {
        // Not an insert or empty, handle via generic path and break batch
        if (nextCmd != null) {
            handleCommand(queue.poll())
        }
        return 
      }
    }
    
    // Log progress every 50k items to prove liveness
    val current = processedCount.get()
    if (current % 50000 == 0) {
      println(s"[EngineManager] Processed $current items...")
    }
  }

  private def handleBatchQuery(firstCmd: QueryCommand): Unit = {
      val batch = new ArrayBuffer[QueryCommand]()
      batch.append(firstCmd)
      
      var ops = 0
      while (!queue.isEmpty && ops < 100) { 
        val next = queue.peek()
        if (next.isInstanceOf[QueryCommand]) {
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
      // [ENT HOOK] We could add "Read Quota" checks here too
      val thresholds = batch.map(_.threshold).toArray
      val results = table.queryShared(thresholds)
      for (i <- batch.indices) {
        batch(i).promise.success(results(i))
      }
  }
  
  private def handleCommand(cmd: EngineCommand): Unit = {
      cmd match {
        case InsertCommand(v) => 
          // Fallback for single inserts mixed in batch
          if (governor.canAcceptInsert("default_tenant")) {
            table.insert(v)
            processedCount.incrementAndGet()
            governor.recordUsage(1, 4)
          }
        case FlushCommand() => table.flush()
        case StopCommand => running.set(false)
        case q: QueryCommand => handleBatchQuery(q)
      }
  }

  def submitInsert(value: Int): Unit = queue.offer(InsertCommand(value))
  def submitFlush(): Unit = queue.offer(FlushCommand())
  def stopEngine(): Unit = queue.offer(StopCommand)
  def submitQuery(threshold: Int): Future[Int] = {
    val p = Promise[Int]()
    queue.offer(QueryCommand(threshold, p))
    p.future
  }
  
  // Debug Helper
  def getQueueSize: Int = queue.size()
}