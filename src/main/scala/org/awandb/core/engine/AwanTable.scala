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
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
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

  // Explicitly track column order for Array-based inserts
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
  
  def addColumn(colName: String, isString: Boolean = false): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (!columns.contains(colName)) {
        columns += (colName -> new NativeColumn(colName, isString))
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

  def insert(colName: String, value: Int): Unit = {
    rwLock.writeLock().lock()
    try {
        if (isClosed) throw new IllegalStateException("Table is closed")
        columns.get(colName).foreach(_.insert(value))
        
        if (columns.size == 1) wal.logInsert(value)
    } finally {
        rwLock.writeLock().unlock()
    }
  }
  
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
    } finally {
        rwLock.writeLock().unlock()
    }
  }

  def insertBatch(values: Array[Int]): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (columns.nonEmpty) {
        val col = columns.values.head
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

      // Optimization: Fast exit if nothing to flush
      val headCol = columns.values.headOption
      if (headCol.isEmpty || headCol.get.isEmpty) return

      // 1. Snapshot Data (Deep Copy to Arrays)
      // We extract the data while holding the lock to ensure atomicity.
      val allColumnsData = columns.values.map { col =>
        if (col.isString) col.toStringArray
        else col.toIntArray
      }.toList
      
      // 2. Persist to Disk (The "Point of No Return")
      // We write to disk BEFORE clearing RAM. 
      // Since we hold the WriteLock, no Readers can see this intermediate state.
      blockManager.createAndPersistBlock(allColumnsData.asInstanceOf[List[Any]])

    } finally {
      // 3. Guaranteed Cleanup [CRITICAL]
      // We clear the RAM buffer in 'finally' to ensure that even if the Disk Write fails 
      // (e.g. IO Error), we DO NOT leave the data in the buffer. 
      // Leaving it would cause the next flush to re-write it, leading to infinite duplication.
      if (!isClosed) {
         columns.values.foreach { col => 
             col.clearDelta()
             
             // Defensive: Force clear if standard reset failed
             if (!col.isEmpty) {
                 col.deltaIntBuffer.clear()
                 col.deltaStringBuffer.clear()
             }
         }
         wal.clear()
      }
      rwLock.writeLock().unlock()
    }
  }

// ---------------------------------------------------------
  // READ PATH (Fully Optimized: RAM + Disk via Morsel Parallelism)
  // ---------------------------------------------------------

  def query(colName: String, search: Any): Int = {
      search match {
          case i: Int => queryInt(i) 
          case s: String => queryString(colName, s)
          case _ => 0
      }
  }

  private def queryString(colName: String, value: String): Int = {
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 
    var colIdx = 0

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       
       columns.get(colName) match {
         case Some(col) =>
           // 1. RAM Optimization: Scan the Delta Buffer (String)
           // Checks the in-memory write buffer before hitting disk
           if (col.deltaStringBuffer.nonEmpty) {
             ramCount = col.deltaStringBuffer.count(_ == value)
           }
           
           // 2. Resolve Column ID for Native Access
           colIdx = columnOrder.indexOf(colName) 
           
           // [CRITICAL SNAPSHOT] 
           // Creates an immutable copy (.toList) of the block pointers.
           // This protects against the "Flush Race Condition" where background flushing 
           // modifies the block list while this query is trying to iterate it.
           snapshotBlocks = blockManager.getLoadedBlocks.toList
           
           // [DEBUG] Enable to verify 16x Duplication Bug is gone (Should match Flush count)
           // println(s"[AwanTable] String Query scanning ${snapshotBlocks.size} blocks")

         case None => return 0
       }
    } finally {
       rwLock.readLock().unlock()
    }

    // 3. Disk Scan (Morsel Parallelism)
    // Distributes blocks across cores. Now safe because 'snapshotBlocks' is immutable.
    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
       NativeBridge.avxScanString(blockPtr, colIdx, value)
    })

    ramCount + diskCount
  }

  def query(threshold: Int): Int = queryInt(threshold)

  private def queryInt(threshold: Int): Int = {
    if (columns.isEmpty) return 0
    val firstColName = columns.keys.head
    
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       
       val col = columns(firstColName)
       if (col.deltaIntBuffer.nonEmpty) {
         ramCount = NativeBridge.avxScanArray(col.deltaIntBuffer.toArray, threshold)
       }
       
       // [CRITICAL FIX] .toList creates an immutable snapshot of the block pointers.
       // This is essential because 'flush()' runs in background and modifies the BlockManager's list.
       snapshotBlocks = blockManager.getLoadedBlocks.toList 
    } finally {
       rwLock.readLock().unlock()
    }

    val colIdx = 0
    
    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
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
       
       // 1. Scan RAM (Delta)
       if (columns.nonEmpty) {
         val firstCol = columns.values.head
         if (firstCol.deltaIntBuffer.nonEmpty) {
           val ramData = firstCol.deltaIntBuffer.toArray
           NativeBridge.avxScanArrayMulti(ramData, thresholds, totalCounts)
         }
       }
       
       // [CRITICAL] Snapshot the list to avoid ConcurrentModificationException during flush
       snapshotBlocks = blockManager.getLoadedBlocks.toList 
     } finally {
       rwLock.readLock().unlock()
     }

     // 2. Scan Disk (Parallel)
     // [UPDATED] Inject the native behavior here. 
     // This tells MorselExec exactly how to allocate memory and how to scan a block.
     val diskCounts = MorselExec.scanSharedParallel(
        snapshotBlocks,
        allocator = () => new Array[Int](thresholds.length), // Thread-local buffer
        scanner = (ptr, counts) => NativeBridge.avxScanMultiBlock(ptr, 0, thresholds, counts) // JNI execution
     )
     
     // 3. Aggregate RAM + Disk
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
      
      engineManager.stopEngine()
      engineManager.joinThread()
      
      blockManager.close()
      wal.close()
      
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