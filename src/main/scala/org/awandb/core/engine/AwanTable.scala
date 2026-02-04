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

class AwanTable(
    val name: String, 
    val capacity: Int, 
    val dataDir: String = "data",
    val governor: EngineGovernor = NoOpGovernor,
    val enableIndex: Boolean = true // <--- [NEW] Control Lazy Indexing
) {
  
  // COMPONENTS
  val wal = new Wal(dataDir)
  
  // Pass the index flag to BlockManager (Threads start here)
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
  def addColumn(colName: String): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (!columns.contains(colName)) {
        columns += (colName -> new NativeColumn(colName))
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
        // [FIX] Use deltaIntBuffer via insert() logic
        col.insert(values(i))
        i += 1
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }
  
  def insert(value: Int): Unit = insertRow(Array(value))

  // [NEW] Explicit Column Insert (for EngineManager)
  def insert(colName: String, value: Int): Unit = {
    rwLock.writeLock().lock()
    try {
        if (isClosed) throw new IllegalStateException("Table is closed")
        // If column exists, insert. If not, ignore (or throw)
        columns.get(colName).foreach(_.insert(value))
        
        // Simple WAL strategy for single-column inserts
        if (columns.size == 1) wal.logInsert(value)
    } finally {
        rwLock.writeLock().unlock()
    }
  }
  
  // [PLACEHOLDER] String support stub for EngineManager
  def insert(colName: String, value: String): Unit = {
     throw new UnsupportedOperationException("String insertion temporarily disabled.")
  }

  // [PERFORMANCE] Batch Insert (Write Fusion)
  def insertBatch(values: Array[Int]): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (columns.nonEmpty) {
        val col = columns.values.head
        
        // [WRITE FUSION]
        // 1. Single IO call to write entire batch to WAL
        wal.logBatch(values)
        
        // 2. Single Memcpy to append to RAM buffer
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
      // [FIX] Check deltaIntBuffer, not deltaBuffer
      if (headCol.isEmpty || headCol.get.deltaIntBuffer.isEmpty) return

      val allColumnsData = columns.values.map(_.toArray).toList
      
      // Persist generates Zone Map metadata automatically
      // Now returns FAST (Disk Only), Indexing happens in background if enableIndex=true
      blockManager.createAndPersistBlock(allColumnsData)

      columns.values.foreach(_.clearDelta())
      wal.clear()
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // READ PATH (Fully Optimized: RAM + Disk via C++)
  // ---------------------------------------------------------
  
  // [NEW] Polymorphic Query Router
  def query(colName: String, search: Any): Int = {
      search match {
          case i: Int => query(i)
          case _ => 0
      }
  }

  def query(threshold: Int): Int = {
    if (columns.isEmpty) return 0
    val firstColName = columns.keys.head
    
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       
       // [RAM OPTIMIZATION] Use C++ kernel for the delta buffer
       val col = columns(firstColName)
       // [FIX] Use deltaIntBuffer
       if (col.deltaIntBuffer.nonEmpty) {
         ramCount = NativeBridge.avxScanArray(col.deltaIntBuffer.toArray, threshold)
       }
       
       snapshotBlocks = blockManager.getLoadedBlocks 
    } finally {
       rwLock.readLock().unlock()
    }

    var diskCount = 0
    val colIdx = 0
    
    // We scan blocks by INDEX to potentially leverage filters in the future
    // (Note: Cuckoo is for Equality, 'threshold' implies Range, so we stick to scan here)
    for (blockPtr <- snapshotBlocks) {
       // [PREDICATE PUSHDOWN]
       // C++ handles Zone Map checks internally (Fast Reject / Fast Accept).
       diskCount += NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0)
    }

    ramCount + diskCount
  }

  def queryShared(thresholds: Array[Int]): Array[Int] = {
     val totalCounts = new Array[Int](thresholds.length)
     var snapshotBlocks: Seq[Long] = Seq.empty
     
     rwLock.readLock().lock()
     try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       if (columns.nonEmpty) {
         val firstCol = columns.values.head
         
         // [RAM OPTIMIZATION] Use C++ kernel for the delta buffer
         // [FIX] Use deltaIntBuffer
         if (firstCol.deltaIntBuffer.nonEmpty) {
           val ramData = firstCol.deltaIntBuffer.toArray
           NativeBridge.avxScanArrayMulti(ramData, thresholds, totalCounts)
         }
       }
       snapshotBlocks = blockManager.getLoadedBlocks
     } finally {
       rwLock.readLock().unlock()
     }

     val colIdx = 0
     
     for (blockPtr <- snapshotBlocks) {
        // [PREDICATE PUSHDOWN & ACCUMULATION]
        // C++ handles Fast Reject / Fast Accept / AVX Fusion internally.
        NativeBridge.avxScanMultiBlock(blockPtr, colIdx, thresholds, totalCounts)
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
    // [FIX] Updated to use correct EngineManager API
    engineManager.joinThread() 
  }

  def persist(dir: String): Unit = flush() 
  def restore(dir: String, count: Int): Unit = {} 
  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = Array.empty[Int]
}