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
import org.awandb.core.query._ 
import java.util.concurrent.{ConcurrentHashMap, Callable}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.collection.Seq 
import org.awandb.core.engine.memory.{EpochManager, NativeMemoryReleaser}
import java.util.Timer
import java.util.TimerTask

class AwanTable(
    val name: String, 
    val capacity: Int, 
    val dataDir: String = "data",
    val governor: EngineGovernor = NoOpGovernor,
    val enableIndex: Boolean = true,
    val daemonIntervalMs: Long = 5000L // Configurable daemon interval
) {
  
  // COMPONENTS
  val wal = new Wal(dataDir)
  val blockManager = new BlockManager(dataDir, enableIndex)
  val columns = new LinkedHashMap[String, NativeColumn]()
  val columnOrder = new ListBuffer[String]()
  
  // [PERFORMANCE] Pre-allocated Buffer
  val resultIndexBuffer: Long = NativeBridge.allocMainStore(capacity)
  
  private[engine] val rwLock = new ReentrantReadWriteLock()
  
  @volatile private var isClosed = false

  // [INDEX] Primary Key Index (Maps ID -> Packed Long (BlockIdx + RowId))
  // Note: For 100% zero-allocation later, swap this with Agrona's Int2LongConcurrentHashMap
  private val primaryIndex = new ConcurrentHashMap[Int, java.lang.Long]()

  // ---------------------------------------------------------
  // BITWISE PACKING HELPERS
  // ---------------------------------------------------------
  @inline private def packLocation(blockIdx: Int, rowId: Int): Long = {
    (blockIdx.toLong << 32) | (rowId & 0xFFFFFFFFL)
  }

  @inline private def unpackBlockIdx(loc: Long): Int = (loc >> 32).toInt
  @inline private def unpackRowId(loc: Long): Int = loc.toInt

  // [DELETION] RAM Deletion Bitmap (For rows in Delta Buffer)
  val ramDeleted = new java.util.BitSet()

  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  // ---------------------------------------------------------
  // ROBUST BACKGROUND DAEMON (EBMM & Compaction)
  // ---------------------------------------------------------
  val epochManager = new org.awandb.core.engine.memory.EpochManager(new org.awandb.core.engine.memory.NativeMemoryReleaser())
  val compactor = new Compactor(this, epochManager)
  
  private val daemonThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (!isClosed) {
        try {
          Thread.sleep(daemonIntervalMs)
          epochManager.advanceGlobalEpoch()
          
          val compacted = compactor.compact(0.3)
          if (compacted > 0) println(s"[Daemon] Compacted $compacted blocks in table '$name'.")
          
          epochManager.tryReclaim()
        } catch {
          case _: InterruptedException => // Graceful shutdown
          case t: Throwable => 
             println(s"[Daemon] FATAL CRASH: ${t.getMessage}")
             t.printStackTrace() // This will reveal if your C++ library needs recompiling!
        }
      }
    }
  }, s"AwanDB-Daemon-$name")
  
  daemonThread.setDaemon(true)
  daemonThread.start()

  blockManager.recover()
  rebuildPrimaryIndex() // [NEW] Rebuild the O(1) index from recovered blocks

  // ---------------------------------------------------------
  // RECOVERY ROUTINE
  // ---------------------------------------------------------
  private def rebuildPrimaryIndex(): Unit = {
    val blocks = blockManager.getLoadedBlocks.toList
    var blockIdx = 0
    
    for (blockPtr <- blocks) {
      val rowCount = NativeBridge.getRowCount(blockPtr)
      if (rowCount > 0) {
        // We know Column 0 is the Primary Key (Int)
        val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
        
        // Fetch the entire ID column in one massive swoop
        val ids = new Array[Int](rowCount)
        NativeBridge.copyToScala(colPtr, ids, rowCount)
        
        // Re-populate the ConcurrentHashMap
        var i = 0
        while (i < rowCount) {
          if (!blockManager.isDeleted(blockPtr, i)) {
            // [FIX] Use packLocation instead of RowLocation case class
            primaryIndex.put(ids(i), packLocation(blockIdx, i))
          }
          i += 1
        }
      }
      blockIdx += 1
    }
  }

  // ---------------------------------------------------------
  // SCHEMA
  // ---------------------------------------------------------
  
  def addColumn(colName: String, isString: Boolean = false, useDictionary: Boolean = false, isVector: Boolean = false): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (!columns.contains(colName)) {
        columns += (colName -> new NativeColumn(colName, isString, useDictionary, isVector))
        columnOrder += colName
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // HELPER: Resolve ID (Fixes ClassCastException)
  // ---------------------------------------------------------
  private def resolveId(key: Any): Int = {
    key match {
      case i: Int => i
      case s: String => s.hashCode // Simple hashing for String PKs
      case _ => key.hashCode
    }
  }

  // ---------------------------------------------------------
  // NEW: Row Access for UPDATE / SELECT
  // ---------------------------------------------------------

  /**
   * Retrieves a full row by ID.
   * Used by UPDATE to perform Read-Modify-Write.
   */
  def getRow(id: Int): Option[Array[Any]] = {
    rwLock.readLock().lock()
    try {
      val packedLoc = primaryIndex.get(id)
      if (packedLoc == null) return None

      val blockIdx = unpackBlockIdx(packedLoc)
      val rowId = unpackRowId(packedLoc)
      
      // Resolve the physical C++ pointer
      val blockPtr = if (blockIdx == -1) 0L else blockManager.getBlockPtr(blockIdx)

      val result = new Array[Any](columns.size)
      var colIdx = 0
      
      for (colName <- columnOrder) {
        val col = columns(colName)
        
        if (blockPtr == 0) {
           // RAM READ
           if (col.isString) {
             result(colIdx) = col.deltaStringBuffer(rowId)
           } else {
             result(colIdx) = col.deltaIntBuffer(rowId)
           }
        } else {
           // DISK READ (Using NativeBridge)
           val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
           val stride = NativeBridge.getColumnStride(blockPtr, colIdx)
           val cellPtr = NativeBridge.getOffsetPointer(colPtr, rowId * stride.toLong)
           // ... (rest of the method remains identical)
           
           if (col.isString) {
             // TODO: String Disk Read support. For now return placeholder.
             result(colIdx) = "N/A (Disk)"
           } else {
             // Read single int
             val tempBuf = new Array[Int](1)
             NativeBridge.copyToScala(cellPtr, tempBuf, 1) // Treats ptr as start of array of 1
             result(colIdx) = tempBuf(0)
           }
        }
        colIdx += 1
      }
      Some(result)
    } finally {
      rwLock.readLock().unlock()
    }
  }

  /**
   * Scans ALL rows (RAM + Disk).
   * Used by SELECT *.
   */
  def scanAll(): Iterator[Array[Any]] = {
      // 1. RAM Iterator
      val ramIter = (0 until columns.values.head.deltaIntBuffer.length).iterator.collect {
         case i if !ramDeleted.get(i) =>
            // Reconstruct Row
            val row = new Array[Any](columns.size)
            var c = 0
            for(colName <- columnOrder) {
               val col = columns(colName)
               if(col.isString) row(c) = col.deltaStringBuffer(i)
               else row(c) = col.deltaIntBuffer(i)
               c += 1
            }
            row
      }

      // 2. Disk Iterator
      val diskIter = blockManager.getLoadedBlocks.iterator.flatMap { blockPtr =>
          val rowCount = NativeBridge.getRowCount(blockPtr)
          (0 until rowCount).iterator.collect {
             case i if !blockManager.isDeleted(blockPtr, i) =>
                 val row = new Array[Any](columns.size)
                 var c = 0
                 for(colName <- columnOrder) {
                     val colPtr = NativeBridge.getColumnPtr(blockPtr, c)
                     val stride = NativeBridge.getColumnStride(blockPtr, c)
                     val cellPtr = NativeBridge.getOffsetPointer(colPtr, i * stride.toLong)
                     val temp = new Array[Int](1)
                     NativeBridge.copyToScala(cellPtr, temp, 1)
                     row(c) = temp(0) // Assume Int for MVP
                     c += 1
                 }
                 row
          }
      }
      
      ramIter ++ diskIter
  }

  // ---------------------------------------------------------
  // CRUD OPERATIONS
  // ---------------------------------------------------------

  def delete(id: Int): Boolean = {
    rwLock.writeLock().lock()
    try {
      val packedLoc = primaryIndex.get(id)
      if (packedLoc == null) return false

      val blockIdx = unpackBlockIdx(packedLoc)
      val rowId = unpackRowId(packedLoc)
      val blockPtr = if (blockIdx == -1) 0L else blockManager.getBlockPtr(blockIdx)

      if (blockPtr == 0) {
        ramDeleted.set(rowId)
      } else {
        blockManager.markDeleted(blockPtr, rowId)
      }
      
      primaryIndex.remove(id)
      true
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // Legacy update stub (redirects to delete, handled by SQLHandler now)
  def update(id: Int, changes: Map[String, Any]): Boolean = delete(id)

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
      
      // [FIX] Use resolveId instead of hard casting to Int.
      // This fixes the ClassCastException in DictionarySpec.
      val id = resolveId(values(0))
      
      val currentRamRowId = if (columns.nonEmpty) columns.values.head.deltaIntBuffer.length else 0

      columnOrder.zipWithIndex.foreach { case (colName, i) =>
        val col = columns(colName)
        val value = values(i)
        
        value match {
          case v: Int => 
            wal.logInsert(v)
            col.insert(v)
          case s: String => 
            if (col.isString) col.insert(s)
            else throw new IllegalArgumentException(s"Column '$colName' expects Int/Vector.")
          case f: Array[Float] =>
            if (col.isVector) col.insert(f)
            else throw new IllegalArgumentException(s"Column '$colName' is not a Vector.")
          case _ => throw new UnsupportedOperationException(s"Type not supported: ${value.getClass}")
        }
      }

      // Use -1 as the blockIdx to represent RAM
      primaryIndex.put(id, packLocation(-1, currentRamRowId))

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
          case Some(col) => if (col.isString) col.insert(value)
          case None => throw new IllegalArgumentException(s"Column '$colName' not found.")
        }
    } finally {
        rwLock.writeLock().unlock()
    }
  }

  // [FIX] Legacy fallback for single-column arrays
  def insertBatch(values: Array[Int]): Unit = {
    if (columnOrder.nonEmpty) insertBatch(columnOrder.head, values)
  }

  // [NEW] Column-aware batch insertion that populates the Primary Index
  def insertBatch(colName: String, values: Array[Int]): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      
      columns.get(colName).foreach { col =>
        // Capture the current row count before inserting
        val startRowId = col.deltaIntBuffer.length
        
        // Log batch to WAL only for the first column to prevent redundancy
        if (columnOrder.head == colName) wal.logBatch(values)
        
        // Physically insert the data into the column array
        col.insertBatch(values)

        // [CRITICAL FIX] If this is the Primary Key column, map the IDs!
        if (columnOrder.head == colName) {
          var i = 0
          while (i < values.length) {
            // Pack location: Block -1 (RAM), Row ID
            primaryIndex.put(values(i), packLocation(-1, startRowId + i))
            i += 1
          }
        }
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

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

  // --- FLUSH ---
  def flush(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) return

      val headCol = columns.values.headOption
      val hasData = headCol.isDefined && !headCol.get.isEmpty
      
      if (hasData) {
          // 1. Prepare Data
          val allColumnsData = columns.values.map { col =>
            if (col.isVector) {
              col.toVectorFlatArray
            } else if (col.isString) {
              if (col.useDictionary) col.encodeDelta() else col.toStringArray
            } else {
              col.toIntArray
            }
          }.toList
          
          // 2. Persist Data Block
          blockManager.createAndPersistBlock(allColumnsData.asInstanceOf[List[Any]])
    
          // 3. Update Index Locations (RAM -> Disk)
          if (blockManager.getLoadedBlocks.nonEmpty) {
              val newBlockIdx = blockManager.getLoadedBlocks.size - 1 // [NEW] Get index
              val newBlockPtr = blockManager.getLoadedBlocks.last
              val rowCount = headCol.get.deltaIntBuffer.length
              val firstCol = columns.values.head
              
              if (!firstCol.isString) {
                  val idColData = firstCol.deltaIntBuffer
                  for (i <- 0 until rowCount) {
                    if (!ramDeleted.get(i)) {
                       val id = idColData(i)
                       // [NEW] Pack the Block Index and Row ID
                       primaryIndex.put(id, packLocation(newBlockIdx, i))
                    } else {
                       primaryIndex.remove(idColData(i))
                       blockManager.markDeleted(newBlockPtr, i)
                    }
                  }
              }
          }
          
          // 4. Cleanup RAM
          if (!isClosed) {
             columns.values.foreach { col => 
                 col.clearDelta()
                 if (!col.isEmpty) { col.deltaIntBuffer.clear(); col.deltaStringBuffer.clear() }
             }
             wal.clear()
             ramDeleted.clear() 
          }
      }

      // 5. Always Save Bitmaps
      blockManager.saveBitmaps()

      // 6. Persist Dictionaries
      columns.values.foreach { col =>
        if (col.useDictionary && col.dictionaryPtr != 0) {
           val dictPath = s"$dataDir/${name}_${col.name}.dict"
           NativeBridge.dictionarySave(col.dictionaryPtr, dictPath)
        }
      }

    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // READ PATH (Filtered Scans)
  // ---------------------------------------------------------

  def getDictionary(colName: String): Option[Long] = {
    columns.get(colName).map(_.dictionaryPtr)
  }

  def query(colName: String, search: Any): Int = {
      search match {
          case i: Int => queryIntEquality(colName, i) 
          case s: String => queryStringEquality(colName, s)
          case _ => 0
      }
  }

  private def queryIntEquality(colName: String, target: Int): Int = {
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 
    var colIdx = 0

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       columns.get(colName) match {
         case Some(col) =>
           colIdx = columnOrder.indexOf(colName)
           if (col.deltaIntBuffer.nonEmpty) {
             val arr = col.deltaIntBuffer.toArray
             var i = 0
             while (i < arr.length) {
               if (!ramDeleted.get(i) && arr(i) == target) ramCount += 1
               i += 1
             }
           }
           snapshotBlocks = blockManager.getLoadedBlocks.toList
         case None => return 0
       }
    } finally {
       rwLock.readLock().unlock()
    }

    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
       if (blockManager.isClean(blockPtr)) {
           // FAST PATH: Manual loop over Clean Block (Avoids Bitmap Check overhead)
           val rowCount = NativeBridge.getRowCount(blockPtr)
           val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
           val data = new Array[Int](rowCount)
           NativeBridge.copyToScala(colPtr, data, rowCount)
           
           var localCount = 0
           var i = 0
           while (i < rowCount) {
             if (data(i) == target) localCount += 1
             i += 1
           }
           localCount
       } else {
           // SLOW PATH: Dirty Block
           val rowCount = NativeBridge.getRowCount(blockPtr)
           val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
           val data = new Array[Int](rowCount)
           NativeBridge.copyToScala(colPtr, data, rowCount)
           
           var localCount = 0
           var i = 0
           while (i < rowCount) {
             if (!blockManager.isDeleted(blockPtr, i)) {
               if (data(i) == target) localCount += 1
             }
             i += 1
           }
           localCount
       }
    })
    ramCount + diskCount
  }

  private def queryStringEquality(colName: String, value: String): Int = {
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 
    var colIdx = 0
    var useDict = false
    var searchId = -1

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       columns.get(colName) match {
         case Some(col) =>
           if (col.deltaStringBuffer.nonEmpty) {
             var i = 0
             while (i < col.deltaStringBuffer.length) {
               if (!ramDeleted.get(i) && col.deltaStringBuffer(i) == value) ramCount += 1
               i += 1
             }
           }
           colIdx = columnOrder.indexOf(colName) 
           snapshotBlocks = blockManager.getLoadedBlocks.toList
           if (col.useDictionary) { 
             useDict = true
             if (col.dictionaryPtr != 0) searchId = NativeBridge.dictionaryEncode(col.dictionaryPtr, value)
           }
         case None => return 0
       }
    } finally {
       rwLock.readLock().unlock()
    }

    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
        val rowCount = NativeBridge.getRowCount(blockPtr)
        var localCount = 0
        if (useDict) {
            val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
            val data = new Array[Int](rowCount)
            NativeBridge.copyToScala(colPtr, data, rowCount)
            
            val isClean = blockManager.isClean(blockPtr)
            var i = 0
            if (isClean) {
                while (i < rowCount) {
                   if (data(i) == searchId) localCount += 1
                   i += 1
                }
            } else {
                while (i < rowCount) {
                   if (!blockManager.isDeleted(blockPtr, i) && data(i) == searchId) localCount += 1
                   i += 1
                }
            }
        } else {
            localCount = NativeBridge.avxScanString(blockPtr, colIdx, value)
        }
        localCount
    })
    ramCount + diskCount
  }

  def query(threshold: Int): Int = {
    if (columns.isEmpty) return 0
    val firstColName = columns.keys.head
    
    var ramCount = 0
    var snapshotBlocks: Seq[Long] = Seq.empty 

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")
       val col = columns(firstColName)
       if (col.deltaIntBuffer.nonEmpty) {
         val arr = col.deltaIntBuffer.toArray
         var i = 0
         while (i < arr.length) {
           if (!ramDeleted.get(i) && arr(i) > threshold) ramCount += 1
           i += 1
         }
       }
       snapshotBlocks = blockManager.getLoadedBlocks.toList 
    } finally {
       rwLock.readLock().unlock()
    }

    val colIdx = 0
    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
       val rowCount = NativeBridge.getRowCount(blockPtr)
       
       // [NEW] Fetch the cached native pointer instantly
       val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
       
       if (bitmaskPtr == 0L) {
           // FAST CLEAN PATH
           NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0)
       } else {
           // FAST DIRTY PATH: Use cached Native Pointer directly
           NativeBridge.avxScanBlockWithDeletions(blockPtr, colIdx, threshold, bitmaskPtr)
       }
    })
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
         if (firstCol.deltaIntBuffer.nonEmpty) {
           val ramData = firstCol.deltaIntBuffer.toArray
           NativeBridge.avxScanArrayMulti(ramData, thresholds, totalCounts)
         }
       }
       snapshotBlocks = blockManager.getLoadedBlocks.toList 
     } finally {
       rwLock.readLock().unlock()
     }

     val diskCounts = MorselExec.scanSharedParallel(
        snapshotBlocks,
        allocator = () => new Array[Int](thresholds.length),
        scanner = (ptr, counts) => NativeBridge.avxScanMultiBlock(ptr, 0, thresholds, counts)
     )
     
     var i = 0
     while (i < totalCounts.length) {
       totalCounts(i) += diskCounts(i)
       i += 1
     }
     
     totalCounts
  }

  def executeGroupBy(keyCol: String, valCol: String): Map[Int, Long] = {
    val keyIdx = columnOrder.indexOf(keyCol)
    val valIdx = columnOrder.indexOf(valCol)
    if (keyIdx == -1 || valIdx == -1) throw new IllegalArgumentException("Column not found")

    val finalMap = scala.collection.mutable.Map[Int, Long]()

    // 1. DISK: Aggregate Native C++ Blocks
    val allBlocks = blockManager.getLoadedBlocks.toSeq
    if (allBlocks.nonEmpty) {
      val cores = MorselExec.activeCores
      val blockSize = math.ceil(allBlocks.size.toDouble / cores).toInt
      val blockChunks = allBlocks.grouped(blockSize).toSeq

      val tasks = blockChunks.map { subset =>
        new Callable[scala.collection.mutable.Map[Int, Long]] {
          override def call(): scala.collection.mutable.Map[Int, Long] = {
            val scanOp = new TableScanOperator(blockManager, subset.toArray, keyIdx, valIdx)
            val aggOp = new HashAggOperator(scanOp)
            aggOp.open()
            
            var resultBatch = aggOp.next() 
            val localMap = scala.collection.mutable.Map[Int, Long]()
            
            // Loop until the operator is exhausted
            while (resultBatch != null && resultBatch.count > 0) {
               val keys = new Array[Int](resultBatch.count)
               val vals = new Array[Long](resultBatch.count)
               NativeBridge.copyToScala(resultBatch.keysPtr, keys, resultBatch.count)
               NativeBridge.copyToScalaLong(resultBatch.valuesPtr, vals, resultBatch.count)
               
               var i = 0
               while (i < resultBatch.count) {
                 val k = keys(i)
                 localMap(k) = localMap.getOrElse(k, 0L) + vals(i)
                 i += 1
               }
               resultBatch = aggOp.next() 
            }
            aggOp.close()
            localMap
          }
        }
      }
      val partialResults = MorselExec.runParallel(tasks)
      for (partial <- partialResults) {
        for ((k, v) <- partial) {
          finalMap(k) = finalMap.getOrElse(k, 0L) + v
        }
      }
    }

    // 2. RAM: Aggregate Unflushed JVM Delta Buffers
    rwLock.readLock().lock()
    try {
      val keyColData = columns(keyCol).deltaIntBuffer
      val valColData = columns(valCol).deltaIntBuffer
      
      var i = 0
      while (i < keyColData.length) {
        if (!ramDeleted.get(i)) {
          val k = keyColData(i)
          val v = valColData(i).toLong
          // Merge RAM values directly into the final C++ Map
          finalMap(k) = finalMap.getOrElse(k, 0L) + v
        }
        i += 1
      }
    } finally {
      rwLock.readLock().unlock()
    }

    finalMap.toMap
  }

  def scanFiltered(colName: String, opType: Int, targetVal: Int): Iterator[Array[Any]] = {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Iterator.empty

    // 1. RAM Filter (Unchanged)
    val ramIter = (0 until columns(colName).deltaIntBuffer.length).iterator.collect {
       case i if !ramDeleted.get(i) =>
         val v = columns(colName).deltaIntBuffer(i)
         val matchFound = opType match {
           case 0 => v == targetVal
           case 1 => v > targetVal
           case 2 => v >= targetVal
           case 3 => v < targetVal
           case 4 => v <= targetVal
           case _ => false
         }
         if (matchFound) {
           val row = new Array[Any](columns.size)
           var c = 0
           for(cn <- columnOrder) {
               val col = columns(cn)
               if(col.isString) row(c) = col.deltaStringBuffer(i)
               else row(c) = col.deltaIntBuffer(i)
               c += 1
           }
           row
         } else null
    }.filter(_ != null)

    // 2. Disk Filter (Predicate Pushdown)
    val snapshotBlocks = blockManager.getLoadedBlocks.toList
    
    val diskIter = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = NativeBridge.getRowCount(blockPtr)
       val outIndicesPtr = NativeBridge.allocMainStore(rowCount)
       
       // [NEW] Fetch the cached pointer instantly from BlockManager
       val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
       
       try {
           val matchCount = NativeBridge.avxFilterBlock(blockPtr, colIdx, opType, targetVal, outIndicesPtr, bitmaskPtr)
           
           if (matchCount == 0) {
               Iterator.empty
           } else {
               // 1. First, pull the exact matched row indices into Scala
               val matchingIndices = new Array[Int](matchCount)
               NativeBridge.copyToScala(outIndicesPtr, matchingIndices, matchCount)
               
               // 2. Allocate Scala arrays to hold the fully extracted columns
               val colsData = new Array[Array[Int]](columnOrder.size)
               
               for (c <- columnOrder.indices) {
                   val col = columns(columnOrder(c))
                   if (!col.isString) { // Safe-guard against native string corruption
                       colsData(c) = new Array[Int](matchCount)
                       val colPtr = NativeBridge.getColumnPtr(blockPtr, c)
                       val tempValuesPtr = NativeBridge.allocMainStore(matchCount)
                       
                       NativeBridge.batchRead(colPtr, outIndicesPtr, matchCount, tempValuesPtr)
                       NativeBridge.copyToScala(tempValuesPtr, colsData(c), matchCount)
                       NativeBridge.freeMainStore(tempValuesPtr)
                   }
               }
               
               // 4. Transpose the columnar arrays into rows purely in Scala (Zero JNI overhead)
               (0 until matchCount).map { i =>
                   val row = new Array[Any](columnOrder.size)
                   for (c <- columnOrder.indices) {
                       row(c) = colsData(c)(i)
                   }
                   row
               }.toIterator
           }
       } finally {
           NativeBridge.freeMainStore(outIndicesPtr)
           // [CRITICAL] Do NOT free bitmaskPtr here anymore! BlockManager owns it.
       }
    }.iterator

    ramIter ++ diskIter
  }

  /**
   * [NEW] Late Materialization Filter
   * Scans a column and returns ONLY the Primary Keys (Row IDs) that match.
   */
  def scanFilteredIds(colName: String, opType: Int, targetVal: Int): Array[Int] = {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Array.empty[Int]
    
    val idColName = columnOrder.head // Assumes Column 0 is the PK

    // 1. RAM Filter (Primitive Array Loop)
    val ramIds = new scala.collection.mutable.ArrayBuffer[Int]()
    val ramCol = columns(colName).deltaIntBuffer
    val ramIdCol = columns(idColName).deltaIntBuffer
    
    var i = 0
    while (i < ramCol.length) {
       if (!ramDeleted.get(i)) {
         val v = ramCol(i)
         val matchFound = opType match {
           case 0 => v == targetVal
           case 1 => v > targetVal
           case 2 => v >= targetVal
           case 3 => v < targetVal
           case 4 => v <= targetVal
           case _ => false
         }
         if (matchFound) ramIds.append(ramIdCol(i))
       }
       i += 1
    }

    // 2. Disk Filter (Predicate Pushdown)
    val snapshotBlocks = blockManager.getLoadedBlocks.toList
    
    val diskIds = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = NativeBridge.getRowCount(blockPtr)
       val outIndicesPtr = NativeBridge.allocMainStore(rowCount)
       val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
       
       try {
           val matchCount = NativeBridge.avxFilterBlock(blockPtr, colIdx, opType, targetVal, outIndicesPtr, bitmaskPtr)
           
           if (matchCount == 0) {
               Array.empty[Int]
           } else {
               // ONLY Gather Column 0 (The Primary Key)
               val idColPtr = NativeBridge.getColumnPtr(blockPtr, 0)
               val tempValuesPtr = NativeBridge.allocMainStore(matchCount)
               
               NativeBridge.batchRead(idColPtr, outIndicesPtr, matchCount, tempValuesPtr)
               
               val ids = new Array[Int](matchCount)
               NativeBridge.copyToScala(tempValuesPtr, ids, matchCount)
               
               NativeBridge.freeMainStore(tempValuesPtr)
               ids
           }
       } finally {
           NativeBridge.freeMainStore(outIndicesPtr)
       }
    }

    ramIds.toArray ++ diskIds
  }

  def swapCompactedBlocks(oldBlocks: Array[Long], newBlockPtr: Long, epochManager: memory.EpochManager): Unit = {
    rwLock.writeLock().lock()
    try {
      // 1. Swap the blocks safely inside BlockManager
      blockManager.swapBlocks(oldBlocks, newBlockPtr)

      // 2. Hand the old blocks to the EpochManager for safe Lock-Free garbage collection!
      oldBlocks.foreach { ptr =>
        epochManager.retire(ptr)
        
        // Also retire the native bitmasks to prevent memory leaks
        val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
        val bitmaskPtr = blockManager.getNativeDeletionBitmap(ptr, rowCount)
        if (bitmaskPtr != 0L) epochManager.retire(bitmaskPtr)
      }
      
      // 3. Rebuild the Primary Index for surviving rows
      if (newBlockPtr != 0L) {
        // We know Column 0 is the Primary Key
        val newBlockIdx = blockManager.getLoadedBlocks.size - 1 
        val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(newBlockPtr)
        val idColPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(newBlockPtr, 0)
        
        val survivingIds = new Array[Int](rowCount)
        org.awandb.core.jni.NativeBridge.copyToScala(idColPtr, survivingIds, rowCount)
        
        var i = 0
        while (i < rowCount) {
          primaryIndex.put(survivingIds(i), packLocation(newBlockIdx, i))
          i += 1
        }
      }
      
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def queryVector(colName: String, query: Array[Float], threshold: Float): Array[Int] = {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Array.empty[Int]
    
    val snapshotBlocks = blockManager.getLoadedBlocks.toList
    
    val rawIds = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(blockPtr)
       val outIndicesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(rowCount)
       
       try {
           val matchCount = org.awandb.core.jni.NativeBridge.avxScanVectorCosine(blockPtr, colIdx, query, threshold, outIndicesPtr)
           
           if (matchCount == 0) {
               Array.empty[Int]
           } else {
               val idColPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, 0)
               val tempValuesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(matchCount)
               org.awandb.core.jni.NativeBridge.batchRead(idColPtr, outIndicesPtr, matchCount, tempValuesPtr)
               
               val ids = new Array[Int](matchCount)
               org.awandb.core.jni.NativeBridge.copyToScala(tempValuesPtr, ids, matchCount)
               org.awandb.core.jni.NativeBridge.freeMainStore(tempValuesPtr)
               ids
           }
       } finally {
           org.awandb.core.jni.NativeBridge.freeMainStore(outIndicesPtr)
       }
    }
    
    // [CRITICAL FIX] Late-Stage Filtering
    // 1. Removes Tombstoned (Deleted) rows by verifying they still exist in the Primary Index.
    // 2. Removes Phantom IDs (ID 0) caused by AVX memory alignment padding.
    rawIds.filter { id =>
      getRow(id).isDefined
    }.distinct.toArray
  }

  // ---------------------------------------------------------
  // GRAPH PROJECTION
  // ---------------------------------------------------------
  
  def projectToGraph(srcColName: String, dstColName: String): org.awandb.core.graph.GraphTable = {
    val srcIdx = columnOrder.indexOf(srcColName)
    val dstIdx = columnOrder.indexOf(dstColName)
    
    if (srcIdx == -1 || dstIdx == -1) {
        throw new IllegalArgumentException("Source or Destination column not found for Graph Projection.")
    }

    // 1. Gather all active edges (Reads from both RAM and Disk, automatically skipping tombstones)
    val edges = scala.collection.mutable.ArrayBuffer[(Int, Int)]()
    val it = scanAll()
    
    while (it.hasNext) {
       val row = it.next()
       val src = row(srcIdx) match { case i: Int => i; case _ => 0 }
       val dst = row(dstIdx) match { case i: Int => i; case _ => 0 }
       edges.append((src, dst))
    }

    if (edges.isEmpty) return new org.awandb.core.graph.GraphTable(0, 0)

    // 2. Determine matrix dimensions
    val maxVertex = edges.map(e => math.max(e._1, e._2)).max
    val numVertices = maxVertex + 1
    val numEdges = edges.size

    // 3. Build the CSR Arrays (Row Pointers and Column Indices)
    val outDegrees = new Array[Int](numVertices)
    for (e <- edges) {
        outDegrees(e._1) += 1
    }

    val rowPtrs = new Array[Int](numVertices + 1)
    var sum = 0
    for (i <- 0 until numVertices) {
      rowPtrs(i) = sum
      sum += outDegrees(i)
    }
    rowPtrs(numVertices) = sum // Tail pointer

    val colIdxs = new Array[Int](numEdges)
    val currentOffsets = rowPtrs.clone() // Tracks insertion positions for each vertex

    for (e <- edges) {
      val src = e._1
      val dst = e._2
      val offset = currentOffsets(src)
      colIdxs(offset) = dst
      currentOffsets(src) += 1
    }

    // 4. Mount into the Native Engine
    val graph = new org.awandb.core.graph.GraphTable(numVertices, numEdges)
    graph.loadFromArrays(rowPtrs, colIdxs)
    
    graph
  }

  def close(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) return
      
      // [CRITICAL FIX] Mark as closed FIRST to break the daemon's while-loop
      isClosed = true 
      
      // Now safely wake it up so it can instantly exit
      daemonThread.interrupt() 
      
      engineManager.stopEngine()
      engineManager.joinThread()
      blockManager.close()
      wal.close()
      columns.values.foreach(_.close())
      org.awandb.core.jni.NativeBridge.freeMainStore(resultIndexBuffer)
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def persist(dir: String): Unit = flush() 
  def restore(dir: String, count: Int): Unit = {} 
  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = Array.empty[Int]
}