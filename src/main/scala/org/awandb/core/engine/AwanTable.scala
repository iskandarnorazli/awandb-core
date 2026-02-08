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

import org.awandb.core.storage.{NativeColumn, BlockManager, Wal}
import org.awandb.core.jni.NativeBridge
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.{LinkedHashMap, ListBuffer} // [FIX] Added ListBuffer
import scala.collection.Seq 
import org.awandb.core.engine.MorselExec

class AwanTable(
    val name: String, 
    val capacity: Int, 
    val dataDir: String = "data",
    val governor: EngineGovernor = NoOpGovernor,
    val enableIndex: Boolean = true // Control Lazy Indexing
) {
  
  // COMPONENTS
  val wal = new Wal(dataDir)
  
  // Pass the index flag to BlockManager
  val blockManager = new BlockManager(dataDir, enableIndex)
  
  val columns = new LinkedHashMap[String, NativeColumn]()

  // [FIX] Explicitly track column order for Array-based inserts
  private val columnOrder = new ListBuffer[String]()
  
  // [PERFORMANCE] Pre-allocated Buffer (Single-Threaded Speedup)
  val resultIndexBuffer: Long = NativeBridge.allocMainStore(capacity)
  
  private val rwLock = new ReentrantReadWriteLock()
  
  // [SAFETY] Gatekeeper
  @volatile private var isClosed = false
  
  // ASYNC MANAGER
  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  blockManager.recover()

  // ---------------------------------------------------------
  // SCHEMA
  // ---------------------------------------------------------
  
  // [UPDATED] Support defining String columns
  def addColumn(colName: String, isString: Boolean = false): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (!columns.contains(colName)) {
        columns += (colName -> new NativeColumn(colName, isString))
        // [FIX] Track the order so insertRow knows which index maps to this column
        columnOrder += colName
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // WRITE PATH
  // ---------------------------------------------------------
  
  def insertRow(values: Array[Any]): Unit = {
    if (values.length != columns.size) {
      throw new IllegalArgumentException(s"Column mismatch: Table has ${columns.size} columns, but row has ${values.length}")
    }

    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Cannot insert: Table is closed.")
      
      // Use columnOrder to map values to columns
      columnOrder.zipWithIndex.foreach { case (colName, i) =>
        val col = columns(colName)
        val value = values(i)
        
        value match {
          case v: Int => 
            wal.logInsert(v)
            col.insert(v)
            
          case s: String => 
            if (col.isString) col.insert(s)
            else throw new IllegalArgumentException(s"Column '$colName' expects Int, but got String.")
            
          case _ => throw new UnsupportedOperationException(s"Type not supported: ${value.getClass}")
        }
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }
  
  def insert(value: Int): Unit = insertRow(Array(value))

  // [UPDATED] Explicit Column Insert (Integer)
  def insert(colName: String, value: Int): Unit = {
    rwLock.writeLock().lock()
    try {
        if (isClosed) throw new IllegalStateException("Table is closed")
        columns.get(colName).foreach(_.insert(value))
        
        // Simple WAL strategy for single-column inserts
        if (columns.size == 1) wal.logInsert(value)
    } finally {
        rwLock.writeLock().unlock()
    }
  }
  
  // [IMPLEMENTED] String Insert Logic
  def insert(colName: String, value: String): Unit = {
    rwLock.writeLock().lock()
    try {
        if (isClosed) throw new IllegalStateException("Table is closed")
        
        columns.get(colName) match {
          case Some(col) => 
            if (col.isString) col.insert(value)
            else throw new IllegalArgumentException(s"Column '$colName' is not a String column.")
          case None => 
            throw new IllegalArgumentException(s"Column '$colName' not found.")
        }
        // Note: WAL for Strings is pending (Phase 4.1 in Roadmap)
    } finally {
        rwLock.writeLock().unlock()
    }
  }

  // [PERFORMANCE] Batch Insert (Write Fusion)
  def insertBatch(values: Array[Int]): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (columns.nonEmpty) {
        val col = columns.values.head
        // [WRITE FUSION]
        wal.logBatch(values)
        col.insertBatch(values)
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // [TEST HELPER] Direct Bulk Load
  def loadDataDirect(data: Array[Int]): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      // ZoneMaps are calculated automatically by C++ during persist
      // Note: This helper only supports Ints for now.
      blockManager.createAndPersistBlock(List(data))
      columns.values.foreach(_.clearDelta())
      wal.clear()
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def flush(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) return

      val headCol = columns.values.headOption
      if (headCol.isEmpty || headCol.get.isEmpty) return

      // [UPDATED] Collect mixed types (Ints and Strings)
      val allColumnsData = columns.values.map { col =>
        if (col.isString) col.toStringArray
        else col.toIntArray
      }.toList
      
      // Persist generates Zone Map metadata automatically
      // Now returns FAST (Disk Only), Indexing happens in background if enableIndex=true
      blockManager.createAndPersistBlock(allColumnsData.asInstanceOf[List[Any]])

      columns.values.foreach(_.clearDelta())
      wal.clear()
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // READ PATH (Fully Optimized: RAM + Disk via Morsel Parallelism)
  // ---------------------------------------------------------

  // [NEW] Polymorphic Query Router
  def query(colName: String, search: Any): Int = {
      search match {
          case i: Int => queryInt(i) // Legacy / Default Int Query
          case s: String => queryString(colName, s)
          case _ => 0
      }
  }

  // [NEW] German String Query Engine (Parallelized)
  private def queryString(colName: String, value: String): Int = {
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 
    var colIdx = 0

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       
       columns.get(colName) match {
         case Some(col) =>
           // 1. RAM Optimization: Scan the Delta Buffer
           if (col.deltaStringBuffer.nonEmpty) {
             ramCount = col.deltaStringBuffer.count(_ == value)
           }
           // 2. Locate Column ID
           colIdx = columnOrder.indexOf(colName) // [FIX] Use columnOrder for index
           snapshotBlocks = blockManager.getLoadedBlocks
           
         case None => return 0
       }
    } finally {
       rwLock.readLock().unlock()
    }

    // 3. Disk Scan (Morsel Parallelism)
    // Distributes blocks across cores to saturate L3 Cache & RAM bandwidth
    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
       NativeBridge.avxScanString(blockPtr, colIdx, value)
    })

    ramCount + diskCount
  }

  // Legacy / Direct Int Query
  def query(threshold: Int): Int = queryInt(threshold)

  private def queryInt(threshold: Int): Int = {
    if (columns.isEmpty) return 0
    val firstColName = columns.keys.head
    
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       
       // [RAM OPTIMIZATION] Use C++ kernel for the delta buffer
       val col = columns(firstColName)
       if (col.deltaIntBuffer.nonEmpty) {
         ramCount = NativeBridge.avxScanArray(col.deltaIntBuffer.toArray, threshold)
       }
       
       snapshotBlocks = blockManager.getLoadedBlocks 
    } finally {
       rwLock.readLock().unlock()
    }

    val colIdx = 0
    
    // [MORSEL PARALLELISM]
    // Replaces sequential loop with ForkJoinPool execution
    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
       // Predicate Pushdown happens inside C++
       NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0)
    })

    ramCount + diskCount
  }

  // [FUSION ENGINE] Parallel Shared Scan
  def queryShared(thresholds: Array[Int]): Array[Int] = {
     val totalCounts = new Array[Int](thresholds.length)
     var snapshotBlocks: Seq[Long] = Seq.empty
     
     rwLock.readLock().lock()
     try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       if (columns.nonEmpty) {
         val firstCol = columns.values.head
         
         // [RAM OPTIMIZATION] Scan Delta Buffer
         if (firstCol.deltaIntBuffer.nonEmpty) {
           val ramData = firstCol.deltaIntBuffer.toArray
           NativeBridge.avxScanArrayMulti(ramData, thresholds, totalCounts)
         }
       }
       snapshotBlocks = blockManager.getLoadedBlocks
     } finally {
       rwLock.readLock().unlock()
     }

     // [MORSEL PARALLELISM]
     // Returns a separate array of counts from the parallel execution
     val diskCounts = MorselExec.scanSharedParallel(snapshotBlocks, thresholds)
     
     // AGGREGATION: Combine RAM results + Disk results
     var i = 0
     while (i < totalCounts.length) {
       totalCounts(i) += diskCounts(i)
       i += 1
     }
     
     totalCounts
  }

  // ---------------------------------------------------------
  // CLEANUP
  // ---------------------------------------------------------
  def close(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) return
      
      // 1. Stop engine first to drain tasks
      engineManager.stopEngine()
      engineManager.joinThread()
      
      // 2. Close storage managers
      blockManager.close()
      wal.close()
      
      // 3. Free manually allocated buffers
      NativeBridge.freeMainStore(resultIndexBuffer)
      
      isClosed = true 
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def persist(dir: String): Unit = flush() 
  def restore(dir: String, count: Int): Unit = {} 
  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = Array.empty[Int]
}