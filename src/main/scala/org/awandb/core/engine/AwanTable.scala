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
import scala.collection.mutable.LinkedHashMap
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
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // WRITE PATH
  // ---------------------------------------------------------
  
  def insertRow(values: Array[Int]): Unit = {
    if (values.length != columns.size) {
      if (columns.size == 1 && values.length == 1) { /* Pass */ } 
      else throw new IllegalArgumentException(s"Column mismatch")
    }

    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Cannot insert: Table is closed.")
      values.foreach(v => wal.logInsert(v))
      var i = 0
      columns.values.foreach { col =>
        col.insert(values(i))
        i += 1
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
      // Note: You must ensure BlockManager.createAndPersistBlock signature accepts generic Seq[Any] 
      // or similar, then handles type checking internally.
      val allColumnsData = columns.values.map { col =>
        if (col.isString) col.toStringArray
        else col.toIntArray
      }.toList
      
      // Persist generates Zone Map metadata automatically
      // Now returns FAST (Disk Only), Indexing happens in background if enableIndex=true
      // CASTING: We assume BlockManager is updated to handle `Seq[Any]` or `Seq[Object]`
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
           // (Temporary: Scala scan until we bind `avxScanStringArray`)
           if (col.deltaStringBuffer.nonEmpty) {
             ramCount = col.deltaStringBuffer.count(_ == value)
           }
           // 2. Locate Column ID
           colIdx = columns.keys.toList.indexOf(colName)
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
      isClosed = true 
      
      engineManager.stopEngine()
      blockManager.close()
      wal.close()
      
      NativeBridge.freeMainStore(resultIndexBuffer)
      
    } finally {
      rwLock.writeLock().unlock()
    }
    engineManager.joinThread() 
  }

  def persist(dir: String): Unit = flush() 
  def restore(dir: String, count: Int): Unit = {} 
  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = Array.empty[Int]
}