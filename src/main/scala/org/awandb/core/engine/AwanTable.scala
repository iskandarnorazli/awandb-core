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

import org.awandb.core.storage.{NativeColumn, BlockManager, Wal}
import org.awandb.core.jni.NativeBridge
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.LinkedHashMap

// NOTE: You need to update EngineManager.scala to accept InsertCommand(values: Array[Int])
// if you want multi-column async inserts. For this snippet, I implemented synchronous insert
// for simplicity, but linked the flush/query to the EngineManager.

class AwanTable(
    val name: String, 
    val capacity: Int, 
    dataDir: String = "data",
    governor: EngineGovernor = NoOpGovernor // [ENT HOOK] Injected Governor
) {
  
  // COMPONENTS
  val wal = new Wal(dataDir)
  val blockManager = new BlockManager(dataDir)
  
  // Use LinkedHashMap to preserve column order for INSERT
  val columns = new LinkedHashMap[String, NativeColumn]()
  
  // CONCURRENCY
  private val rwLock = new ReentrantReadWriteLock()
  
  // GOVERNANCE (The EngineManager)
  // Pass 'this' and the injected 'governor' so the Manager can enforce quotas
  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  // ---------------------------------------------------------
  // SCHEMA MANAGEMENT
  // ---------------------------------------------------------
  def addColumn(colName: String): Unit = {
    columns += (colName -> new NativeColumn(colName))
  }

  // ---------------------------------------------------------
  // WRITE PATH
  // ---------------------------------------------------------
  
  /**
   * Insert a full row (Thread-Safe).
   * Example: insertRow(Array(101, 50, 200)) // ID, Price, Stock
   */
  def insertRow(values: Array[Int]): Unit = {
    if (values.length != columns.size) {
      throw new IllegalArgumentException(s"Column mismatch: Expected ${columns.size}, got ${values.length}")
    }

    rwLock.writeLock().lock()
    try {
      // 1. Write to WAL (Durability)
      // We log each value. In production, you'd serialize the whole row.
      values.foreach(v => wal.logInsert(v))

      // 2. Write to RAM (Delta)
      var i = 0
      columns.values.foreach { col =>
        col.insert(values(i))
        i += 1
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }
  
  // Used by EngineManager for single-value compatibility
  def insert(value: Int): Unit = insertRow(Array(value))

  /**
   * Flush RAM to Disk (Create Immutable Block).
   */
  def flush(): Unit = {
    rwLock.writeLock().lock()
    try {
      // Check if we have data
      val rowCount = columns.values.headOption.map(_.deltaBuffer.size).getOrElse(0)
      if (rowCount == 0) return

      // 1. Gather all columns
      val allColumnsData = columns.values.map(_.toArray).toSeq

      // 2. Persist via BlockManager
      blockManager.createAndPersistBlock(allColumnsData)

      // 3. Clear RAM & WAL
      columns.values.foreach(_.clearDelta())
      wal.clear()
      
      println(s"[AwanTable] Flushed $rowCount rows to disk.")
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // READ PATH
  // ---------------------------------------------------------

  /**
   * Simple Query: Count rows where col > threshold
   */
  def query(threshold: Int): Int = {
    // For simplicity, we query the FIRST column
    if (columns.isEmpty) return 0
    val firstColName = columns.keys.head
    
    // 1. Scan RAM
    rwLock.readLock().lock()
    var ramCount = 0
    try {
       ramCount = columns(firstColName).deltaBuffer.count(_ > threshold)
    } finally {
       rwLock.readLock().unlock()
    }

    // 2. Scan Disk Blocks
    var diskCount = 0
    val loadedBlocks = blockManager.getLoadedBlocks
    
    // We assume Column 0 is the one we want to filter
    val colIdx = 0 
    
    for (blockPtr <- loadedBlocks) {
       val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
       val rows = NativeBridge.getRowCount(blockPtr)
       
       // Allocate temp buffer for results (just to count them)
       val outPtr = NativeBridge.allocMainStore(rows)
       diskCount += NativeBridge.avxScanIndices(colPtr, rows, threshold, outPtr)
       NativeBridge.freeMainStore(outPtr)
    }

    ramCount + diskCount
  }
  
  /**
   * Shared Scan (Query Fusion)
   */
  def queryShared(thresholds: Array[Int]): Array[Int] = {
     val totalCounts = new Array[Int](thresholds.length)
     
     // 1. RAM Scan (Naive loop for now)
     rwLock.readLock().lock()
     try {
       if (columns.nonEmpty) {
         val firstCol = columns.values.head
         for (x <- firstCol.deltaBuffer) {
           for (i <- thresholds.indices) {
             if (x > thresholds(i)) totalCounts(i) += 1
           }
         }
       }
     } finally {
       rwLock.readLock().unlock()
     }

     // 2. Disk Scan (Optimized AVX)
     val blockCounts = new Array[Int](thresholds.length)
     for (blockPtr <- blockManager.getLoadedBlocks) {
        val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
        val rows = NativeBridge.getRowCount(blockPtr)
        
        NativeBridge.avxScanIndicesMulti(colPtr, rows, thresholds, blockCounts)
        
        for (i <- thresholds.indices) totalCounts(i) += blockCounts(i)
     }
     
     totalCounts
  }
  
  // ---------------------------------------------------------
  // CLEANUP
  // ---------------------------------------------------------
  def close(): Unit = {
    engineManager.stopEngine()
    engineManager.join()
    blockManager.close()
    wal.close()
  }

  // ---------------------------------------------------------
  // LEGACY / COMPATIBILITY HELPERS
  // ---------------------------------------------------------
  
  def persist(dir: String): Unit = flush() // Alias for tests

  def restore(dir: String, count: Int): Unit = {} 

  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = {
      Array.empty[Int]
  }
}