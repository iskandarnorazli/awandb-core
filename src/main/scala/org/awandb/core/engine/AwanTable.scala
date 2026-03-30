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
  
  // Track Vector Dimensions dynamically to bypass C++ Stride defaults
  private val vectorDims = new ConcurrentHashMap[String, Int]()
  
  // [PERFORMANCE] Pre-allocated Buffer
  var resultIndexBuffer: Long = NativeBridge.allocMainStore(capacity)
  
  // [CRITICAL FIX] Track this memory!
  org.awandb.core.engine.memory.NativeMemoryTracker.recordAllocation(resultIndexBuffer, capacity * 4L)
  
  val rwLock = new ReentrantReadWriteLock()
  
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

  // ---------------------------------------------------------
  // EPOCH MEMORY PROTECTION (EBMM)
  // ---------------------------------------------------------
  @inline private[awandb] def withEpoch[T](block: => T): T = {
    val threadId = Thread.currentThread().getId
    epochManager.registerThread(threadId)
    try {
      block
    } finally {
      epochManager.deregisterThread(threadId)
    }
  }

  // [DELETION] RAM Deletion Bitmap (For rows in Delta Buffer)
  val ramDeleted = new java.util.BitSet()

  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  // ---------------------------------------------------------
  // ROBUST BACKGROUND DAEMON (EBMM & Compaction & Pacemaker)
  // ---------------------------------------------------------
  val epochManager = new org.awandb.core.engine.memory.EpochManager(new org.awandb.core.engine.memory.NativeMemoryReleaser())
  val compactor = new Compactor(this, epochManager)
  
  private val daemonThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (!isClosed) {
        try {
          Thread.sleep(daemonIntervalMs)
          epochManager.advanceGlobalEpoch()
          
          // 1. [PHASE 6] Pacemaker Daemon: Clock-Sweep LRU Eviction
          // Triggers gentle eviction down to 20% Idle Watermark
          NativeBridge.triggerPacemakerSweep()

          // 2. Build pending Cuckoo Filters
          var built = 0
          val isVectorFlags = columnOrder.map(c => columns(c).isVector).toArray
          while (blockManager.buildPendingIndexes(isVectorFlags) > 0) { built += 1 }
          
          // 3. Compact dead blocks
          val compacted = compactor.compact(0.3)
          if (compacted > 0) println(s"[Daemon] Compacted $compacted blocks in table '$name'.")
          
          // 4. Reclaim memory safely
          epochManager.tryReclaim()
        } catch {
          case _: InterruptedException => // Graceful shutdown
          case t: Throwable => 
             println(s"[Daemon] FATAL CRASH: ${t.getMessage}")
             t.printStackTrace() 
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
    
    // 1. Map all Disk Rows
    for (blockPtr <- blocks) {
      val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(blockPtr)
      if (rowCount > 0) {
        // We know Column 0 is the Primary Key (Int)
        val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, 0)
        
        // Fetch the entire ID column in one massive swoop
        val ids = new Array[Int](rowCount)
        org.awandb.core.jni.NativeBridge.copyToScala(colPtr, ids, rowCount)
        
        // Re-populate the ConcurrentHashMap
        var i = 0
        while (i < rowCount) {
          if (!blockManager.isDeleted(blockPtr, i)) {
            primaryIndex.put(ids(i), packLocation(blockIdx, i))
          }
          i += 1
        }
      }
      blockIdx += 1
    }

    // 2. [NEW] Restore surviving RAM Rows back into the Index!
    if (columns.nonEmpty) {
      val ramIds = columns.values.head.deltaIntBuffer
      var i = 0
      while (i < ramIds.length) {
        if (!ramDeleted.get(i)) {
          // Pack location: Block -1 (RAM), Row ID
          primaryIndex.put(ramIds(i), packLocation(-1, i))
        }
        i += 1
      }
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
   * Used by UPDATE to perform Read-Modify-Write and by SELECT for late materialization.
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
           if (col.isVector) {
             result(colIdx) = col.deltaVectorBuffer(rowId)
           } else if (col.isString) {
             result(colIdx) = col.deltaStringBuffer(rowId)
           } else {
             result(colIdx) = col.deltaIntBuffer(rowId)
           }
        } else {
           // DISK READ (Using NativeBridge)
           val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
           
           // 🚀 FIX: Override the C++ Stride dynamically for AI Vectors
           val baseStride = NativeBridge.getColumnStride(blockPtr, colIdx)
           val stride = if (col.isVector) vectorDims.getOrDefault(colName, math.max(1, baseStride / 4)) * 4 else baseStride
           
           val cellPtr = NativeBridge.getOffsetPointer(colPtr, rowId * stride.toLong)
           
           if (col.isVector) {
             // Calculate vector dimension (Stride is in bytes, Float is 4 bytes)
             val dim = stride / 4
             val tempBuf = new Array[Float](dim)
             NativeBridge.copyToScalaFloat(cellPtr, tempBuf, dim)
             result(colIdx) = tempBuf
           } else if (col.isString) {
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
  def scanAll(): Iterator[Array[Any]] = withEpoch {
      // 1. RAM Iterator (Forced eager evaluation with .toList)
      val ramList = (0 until columns.values.head.deltaIntBuffer.length).collect {
         case i if !ramDeleted.get(i) =>
            val row = new Array[Any](columns.size)
            var c = 0
            for(colName <- columnOrder) {
               val col = columns(colName)
               if(col.isVector) row(c) = col.deltaVectorBuffer(i)
               else if(col.isString) row(c) = col.deltaStringBuffer(i)
               else row(c) = col.deltaIntBuffer(i)
               c += 1
            }
            row
      }.toList

      // 2. Disk Iterator (Forced eager evaluation with .toList)
      val diskList = blockManager.getLoadedBlocks.toList.flatMap { blockPtr =>
          val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(blockPtr)
          (0 until rowCount).collect {
             case i if !blockManager.isDeleted(blockPtr, i) =>
                 val row = new Array[Any](columns.size)
                 var c = 0
                 for(colName <- columnOrder) {
                     val col = columns(colName)
                     val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, c)
                     
                     // 🚀 FIX: Safe Stride calculation for Full Scans
                     val baseStride = org.awandb.core.jni.NativeBridge.getColumnStride(blockPtr, c)
                     val stride = if (col.isVector) vectorDims.getOrDefault(colName, math.max(1, baseStride / 4)) * 4 else baseStride
                     
                     val cellPtr = org.awandb.core.jni.NativeBridge.getOffsetPointer(colPtr, i * stride.toLong)
                     
                     if (col.isVector) {
                         val dim = stride / 4
                         val tempBuf = new Array[Float](dim)
                         org.awandb.core.jni.NativeBridge.copyToScalaFloat(cellPtr, tempBuf, dim)
                         row(c) = tempBuf
                     } else if (col.isString) {
                         row(c) = "N/A (Disk)"
                     } else {
                         val temp = new Array[Int](1)
                         org.awandb.core.jni.NativeBridge.copyToScala(cellPtr, temp, 1)
                         row(c) = temp(0)
                     }
                     c += 1
                 }
                 row
          }
      }
      
      (ramList ++ diskList).iterator
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

  // Column-aware batch insertion that populates the Primary Index
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

  // ---------------------------------------------------------
  // 🚀 ZERO-COPY INGESTION PATH
  // ---------------------------------------------------------
  def insertBatchFromPointer(colName: String, dataPtr: Long, rowCount: Int): Unit = {
    // 1. Allocate a single contiguous JVM array
    val arr = new Array[Int](rowCount)
    
    // 2. Blast the data from Arrow's off-heap memory straight into the array via JNI
    org.awandb.core.jni.NativeBridge.copyToScala(dataPtr, arr, rowCount)
    
    // 3. Route to the standard batch insert to ensure WAL logging and Primary Indexing
    insertBatch(colName, arr)
  }

  // ---------------------------------------------------------
  // TRUE ZERO-COPY BULK INGESTION (Bypasses WAL & Delta Store)
  // ---------------------------------------------------------
  def bulkLoadFromArrowPointers(
      colNames: Array[String], 
      colTypesIn: Array[Int], 
      dataPtrsIn: Array[Long], 
      offsetPtrsIn: Array[Long], 
      sizesIn: Array[Int], 
      dimsIn: Array[Int], 
      rowCount: Int
  ): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")

      val colCount = columnOrder.size
      val alignedTypes = new Array[Int](colCount)
      val alignedData = new Array[Long](colCount)
      val alignedOffsets = new Array[Long](colCount)
      val alignedSizes = new Array[Int](colCount)
      val alignedDims = new Array[Int](colCount)

      // Align incoming Arrow columns to the physical C++ Block column order
      for (i <- columnOrder.indices) {
        val name = columnOrder(i)
        val inIdx = colNames.indexOf(name)
        if (inIdx == -1) throw new IllegalArgumentException(s"Missing column in bulk load: $name")
        
        alignedTypes(i) = colTypesIn(inIdx)
        alignedData(i) = dataPtrsIn(inIdx)
        alignedOffsets(i) = offsetPtrsIn(inIdx)
        alignedSizes(i) = sizesIn(inIdx)
        alignedDims(i) = dimsIn(inIdx)
        
        // FIX: Cache the exact dimension size for Future Reads!
        if (alignedTypes(i) == 3 && alignedDims(i) > 0) {
            vectorDims.put(name, alignedDims(i))
        }
      }

      // Delegate to BlockManager for 100% Native C++ block creation & memcpy
      val blockPtr = blockManager.createAndPersistBlockFromPointers(
          rowCount, alignedTypes, alignedData, alignedOffsets, alignedSizes, alignedDims
      )

      // [CRITICAL FIX] Bind the native vector metadata directly to the Arrow-copied block!
      if (blockPtr != 0L) {
          for (i <- columnOrder.indices) {
              if (alignedTypes(i) == 3 && alignedDims(i) > 0) {
                  org.awandb.core.jni.NativeBridge.setVectorDimNative(blockPtr, i, alignedDims(i))
              }
          }
      }

      // Update Primary Index (Fetch ONLY the ID column into the JVM)
      if (blockPtr != 0L && !columns.values.head.isString && !columns.values.head.isVector) {
        val idColIdx = 0 // Assuming Col 0 is PK
        val idDestPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, idColIdx)

        val idArray = new Array[Int](rowCount)
        org.awandb.core.jni.NativeBridge.copyToScala(idDestPtr, idArray, rowCount)

        val newBlockIdx = blockManager.getLoadedBlocks.size - 1

        var i = 0
        while (i < rowCount) {
          primaryIndex.put(idArray(i), packLocation(newBlockIdx, i))
          i += 1
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
              val newBlockIdx = blockManager.getLoadedBlocks.size - 1
              val newBlockPtr = blockManager.getLoadedBlocks.last
              
              // [CRITICAL FIX] Track the newly flushed native block memory!
              val blockSize = org.awandb.core.jni.NativeBridge.getBlockSize(newBlockPtr)
              org.awandb.core.engine.memory.NativeMemoryTracker.recordAllocation(newBlockPtr, blockSize)
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
      // 1. Generate a unique Query ID for the C++ Memory Arena
      val queryId = java.util.UUID.randomUUID().toString
      
      // 2. Initialize the C++ Query Context
      NativeBridge.initQueryContext(queryId)
      
      try {
          // 3. Dispatch to the appropriate typed execution path
          search match {
              case i: Int => queryIntEquality(colName, i) 
              case s: String => queryStringEquality(colName, s)
              case _ => 0
          }
      } finally {
          // 4. CRITICAL: Guarantee arena destruction even if the scan fails
          NativeBridge.destroyQueryContext(queryId)
      }
  }

  private def queryIntEquality(colName: String, target: Int): Int = withEpoch {
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
       
       val blockIdx = blockManager.getLoadedBlocks.indexOf(blockPtr)
       
       // [NEW] Check 1: Zone Map Short-Circuit
       val (min, max) = if (blockIdx != -1) blockManager.getZoneMap(blockIdx, colIdx) else (Int.MinValue, Int.MaxValue)
       
       if (target < min || target > max) {
           0 // FASTEST PATH: Target is completely out of bounds. Skip AVX scan!
       } 
       // [NEW] Check 2: Multi-Column Cuckoo Filter Short-Circuit
       else if (blockIdx != -1 && !blockManager.mightContain(blockIdx, colIdx, target)) {
           0 // FAST PATH: Filter says the key is definitely not here. Skip AVX scan!
       } 
       else if (blockManager.isClean(blockPtr)) {
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

  private def queryStringEquality(colName: String, value: String): Int = withEpoch {
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

  def query(threshold: Int): Int = withEpoch {
    if (columns.isEmpty) return 0
    
    // 1. Generate a unique Query ID for this execution's C++ Memory Arena
    val queryId = java.util.UUID.randomUUID().toString
    
    // 2. Initialize the C++ Query Context
    NativeBridge.initQueryContext(queryId)
    
    try {
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
         
         // Fetch the cached native pointer instantly
         val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
         
         // Note: If avxScanBlock ever needs to allocate temporary memory, 
         // you can eventually pass `queryId` into it here.
         if (bitmaskPtr == 0L) {
             // FAST CLEAN PATH
             NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0)
         } else {
             // FAST DIRTY PATH: Use cached Native Pointer directly
             NativeBridge.avxScanBlockWithDeletions(blockPtr, colIdx, threshold, bitmaskPtr)
         }
      })
      
      ramCount + diskCount
      
    } finally {
      // 3. CRITICAL: Destroy the C++ arena, guaranteed to run even if MorselExec throws
      NativeBridge.destroyQueryContext(queryId)
    }
  }

  def queryShared(thresholds: Array[Int]): Array[Int] = withEpoch {
    // 1. Generate a unique Query ID for this execution's C++ Memory Arena
    val queryId = java.util.UUID.randomUUID().toString
    
    // 2. Initialize the C++ Query Context
    NativeBridge.initQueryContext(queryId)
    
    try {
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
    } finally {
      // 3. CRITICAL: Destroy the C++ arena, guaranteed to run even if MorselExec throws
      NativeBridge.destroyQueryContext(queryId)
    }
  }

  // ---------------------------------------------------------
  // STANDARD HASH GROUP BY
  // ---------------------------------------------------------
  def executeGroupBy(keyCol: String, valCol: String, aggFunc: String = "SUM"): Map[Int, Long] = withEpoch {
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
            // [FIX] Pass the aggFunc down to the operator
            val aggOp = new HashAggOperator(scanOp, aggFunc)
            
            try {
              aggOp.open()
              
              var resultBatch = aggOp.next() 
              val localMap = scala.collection.mutable.Map[Int, Long]()
              
              // Loop until the operator is exhausted
              while (resultBatch != null) {
                 if (resultBatch.count > 0) {
                     val keys = new Array[Int](resultBatch.count)
                     val vals = new Array[Long](resultBatch.count)
                     NativeBridge.copyToScala(resultBatch.keysPtr, keys, resultBatch.count)
                     NativeBridge.copyToScalaLong(resultBatch.valuesPtr, vals, resultBatch.count)
                     
                     var i = 0
                     while (i < resultBatch.count) {
                       val k = keys(i)
                       val v = vals(i) // [FIX] Values are completely accurate from C++ natively now
                       localMap(k) = localMap.getOrElse(k, 0L) + v
                       i += 1
                     }
                 }
                 
                 resultBatch = aggOp.next() 
              }
              localMap
            } finally {
              // [CRITICAL FIX] Only close the ROOT operator!
              aggOp.close()
            }
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
      val keyColObj = columns(keyCol)
      val valColObj = columns(valCol)
      
      // [FIX] Route to the correct Delta Buffer depending on column type
      val numRows = if (keyColObj.isString) keyColObj.deltaStringBuffer.length else keyColObj.deltaIntBuffer.length
      
      var i = 0
      while (i < numRows) {
        if (!ramDeleted.get(i)) {
          // [FIX] Extract the dictionary ID directly from the String buffer
          val k = if (keyColObj.isString) keyColObj.getDictId(keyColObj.deltaStringBuffer(i)) else keyColObj.deltaIntBuffer(i)
          
          if (k != -1) {
              val v = if (aggFunc == "COUNT") 1L else valColObj.deltaIntBuffer(i).toLong
              finalMap(k) = finalMap.getOrElse(k, 0L) + v
          }
        }
        i += 1
      }
    } finally {
      rwLock.readLock().unlock()
    }

    finalMap.toMap
  }

  // ---------------------------------------------------------
  // PHASE 4: FAST-PATH DICTIONARY GROUP BY
  // ---------------------------------------------------------
  def executeDictionaryGroupBy(keyCol: String, valCol: String, aggFunc: String = "SUM"): Map[Int, Long] = withEpoch {
    val keyIdx = columnOrder.indexOf(keyCol)
    val valIdx = columnOrder.indexOf(valCol)
    if (keyIdx == -1 || valIdx == -1) throw new IllegalArgumentException("Column not found")

    // Retrieve exactly how much memory the Array Aggregator needs to allocate
    val dictSize = columns(keyCol).getDictionarySize()
    if (dictSize == 0) return Map.empty[Int, Long]
    val maxDictId = dictSize - 1

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
            
            // [NEW] Use the O(1) Array Operator instead of the Hash Operator and pass aggFunc
            val aggOp = new ArrayAggOperator(scanOp, maxDictId, aggFunc) 
            
            try {
              aggOp.open()
              var resultBatch = aggOp.next() 
              val localMap = scala.collection.mutable.Map[Int, Long]()
              
              while (resultBatch != null) {
                 if (resultBatch.count > 0) {
                     val keys = new Array[Int](resultBatch.count)
                     val vals = new Array[Long](resultBatch.count)
                     NativeBridge.copyToScala(resultBatch.keysPtr, keys, resultBatch.count)
                     NativeBridge.copyToScalaLong(resultBatch.valuesPtr, vals, resultBatch.count)
                     
                     var i = 0
                     while (i < resultBatch.count) {
                       val k = keys(i)
                       val v = vals(i) // [FIX] Values are completely accurate from C++ natively now
                       localMap(k) = localMap.getOrElse(k, 0L) + v
                       i += 1
                     }
                 }
                 resultBatch = aggOp.next() 
              }
              localMap
            } finally {
              aggOp.close()
            }
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
      val keyColObj = columns(keyCol)
      val valColObj = columns(valCol)
      
      // [FIX] Route to the correct Delta Buffer depending on column type
      val numRows = if (keyColObj.isString) keyColObj.deltaStringBuffer.length else keyColObj.deltaIntBuffer.length
      
      var i = 0
      while (i < numRows) {
        if (!ramDeleted.get(i)) {
          // [FIX] Extract the dictionary ID directly from the String buffer
          val k = if (keyColObj.isString) keyColObj.getDictId(keyColObj.deltaStringBuffer(i)) else keyColObj.deltaIntBuffer(i)
          
          if (k != -1) {
              val v = if (aggFunc == "COUNT") 1L else valColObj.deltaIntBuffer(i).toLong
              finalMap(k) = finalMap.getOrElse(k, 0L) + v
          }
        }
        i += 1
      }
    } finally {
      rwLock.readLock().unlock()
    }

    finalMap.toMap
  }

  // ---------------------------------------------------------
  // COMPOSITE PREDICATE PUSHDOWN (AST / RPN)
  // ---------------------------------------------------------

  def executeCompositeFilter(rpnInstructions: Array[Int]): Array[Int] = withEpoch {
    val ramIds = new scala.collection.mutable.ArrayBuffer[Int]()
    var snapshotBlocks: List[Long] = Nil

    rwLock.readLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")

      // 1. Pre-process Materialized ASTs (Opcode 4) into HashSets to avoid O(N^2) lookups
      val matSets = scala.collection.mutable.Map[Int, Set[Int]]()
      var pc = 0
      while (pc < rpnInstructions.length) {
        val op = rpnInstructions(pc)
        if (op == 1) pc += 4
        else if (op == 2 || op == 3) pc += 1
        else if (op == 4) {
          val len = rpnInstructions(pc + 1)
          val slice = rpnInstructions.slice(pc + 2, pc + 2 + len)
          matSets(pc) = slice.toSet
          pc += 2 + len
        } else {
          throw new RuntimeException(s"Unknown RPN Opcode: $op")
        }
      }

      // 2. RAM Evaluator (Evaluates the RPN stack per row)
      val pkCol = columns(columnOrder.head).deltaIntBuffer
      val numRows = pkCol.length
      
      var i = 0
      while (i < numRows) {
        if (!ramDeleted.get(i)) {
          var execPc = 0
          val stack = new java.util.BitSet() 
          var sp = 0

          while (execPc < rpnInstructions.length) {
            val op = rpnInstructions(execPc)
            if (op == 1) { // PREDICATE: [1, colIdx, opType, targetVal]
              val colIdx = rpnInstructions(execPc + 1)
              val opType = rpnInstructions(execPc + 2)
              val targetVal = rpnInstructions(execPc + 3)
              
              val v = columns(columnOrder(colIdx)).deltaIntBuffer(i)
              val res = opType match {
                case 0 => v == targetVal
                case 1 => v > targetVal
                case 2 => v >= targetVal
                case 3 => v < targetVal
                case 4 => v <= targetVal
                case _ => false
              }
              stack.set(sp, res)
              sp += 1
              execPc += 4
            } else if (op == 2) { // AND: [2]
              val b2 = stack.get(sp - 1)
              val b1 = stack.get(sp - 2)
              sp -= 2
              stack.set(sp, b1 && b2)
              sp += 1
              execPc += 1
            } else if (op == 3) { // OR: [3]
              val b2 = stack.get(sp - 1)
              val b1 = stack.get(sp - 2)
              sp -= 2
              stack.set(sp, b1 || b2)
              sp += 1
              execPc += 1
            } else if (op == 4) { // MATERIALIZED: [4, len, id1, id2...]
              val len = rpnInstructions(execPc + 1)
              val rowId = pkCol(i)
              val res = matSets(execPc).contains(rowId)
              stack.set(sp, res)
              sp += 1
              execPc += 2 + len
            }
          }

          if (sp > 0 && stack.get(0)) {
            ramIds.append(pkCol(i))
          }
        }
        i += 1
      }

      snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally {
      rwLock.readLock().unlock()
    }

    // 3. DISK Evaluator (JNI AVX Pushdown)
    val diskIds = snapshotBlocks.flatMap { blockPtr =>
      val rowCount = NativeBridge.getRowCount(blockPtr)
      val outIndicesSize = rowCount * 4L
      val outIndicesPtr = NativeBridge.allocMainStore(outIndicesSize) // 4 bytes per Int
      
      // [CRITICAL FIX] Track the allocation
      org.awandb.core.engine.memory.NativeMemoryTracker.recordAllocation(outIndicesPtr, outIndicesSize)
      
      val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)

      try {
        val matchCount = NativeBridge.avxCompositeFilter(blockPtr, rpnInstructions, outIndicesPtr, bitmaskPtr)

        if (matchCount == 0) {
          Array.empty[Int]
        } else {
          // Extract only the matching Primary Keys (Col 0)
          val idColPtr = NativeBridge.getColumnPtr(blockPtr, 0)
          
          val tempValuesSize = matchCount * 4L
          val tempValuesPtr = NativeBridge.allocMainStore(tempValuesSize)
          
          // [CRITICAL FIX] Track the transient buffer allocation
          org.awandb.core.engine.memory.NativeMemoryTracker.recordAllocation(tempValuesPtr, tempValuesSize)
          
          try {
            NativeBridge.batchRead(idColPtr, outIndicesPtr, matchCount, tempValuesPtr)
            val ids = new Array[Int](matchCount)
            NativeBridge.copyToScala(tempValuesPtr, ids, matchCount)
            ids
          } finally {
            // [CRITICAL FIX] Ensure free and untrack even if copyToScala throws
            NativeBridge.freeMainStore(tempValuesPtr)
            org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(tempValuesPtr)
          }
        }
      } finally {
        // Untrack the primary indices buffer
        NativeBridge.freeMainStore(outIndicesPtr)
        org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(outIndicesPtr)
      }
    }
    
    ramIds.toArray ++ diskIds
  }

  // ---------------------------------------------------------
  // NATIVE MATH PUSHDOWN (UPDATE)
  // ---------------------------------------------------------
  def executeMathUpdate(colName: String, opChar: Char, operand: Int, matchedIds: Array[Int]): Unit = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1 || columns(colName).isString || columns(colName).isVector) return

    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")

      // Group IDs by their physical block location
      val blockGroups = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Int]]()
      
      var i = 0
      while (i < matchedIds.length) {
        val packedLoc = primaryIndex.get(matchedIds(i))
        if (packedLoc != null) {
          val bIdx = unpackBlockIdx(packedLoc)
          val rId = unpackRowId(packedLoc)
          blockGroups.getOrElseUpdate(bIdx, scala.collection.mutable.ArrayBuffer[Int]()).append(rId)
        }
        i += 1
      }

      // 1. Update RAM (Block -1)
      blockGroups.get(-1).foreach { ramRowIds =>
        val ramBuffer = columns(colName).deltaIntBuffer
        ramRowIds.foreach { rId =>
          if (!ramDeleted.get(rId)) {
            opChar match {
              case '+' => ramBuffer(rId) += operand
              case '-' => ramBuffer(rId) -= operand
              case '*' => ramBuffer(rId) *= operand
              case '/' => if (operand != 0) ramBuffer(rId) /= operand
            }
          }
        }
      }

      // 2. Update Disk Blocks via Native AVX Engine
      blockGroups.foreach { case (bIdx, diskRowIds) =>
        if (bIdx != -1) {
          val blockPtr = blockManager.getBlockPtr(bIdx)
          if (blockPtr != 0L) {
             val matchCount = diskRowIds.length
             
             // [CRITICAL FIX] Multiply by 4 bytes per Int to prevent a C++ Buffer Overflow!
             val tempIndicesPtr = NativeBridge.allocMainStore(matchCount * 4) 
             
             try {
                // Copy the specific Row IDs to a C++ buffer
                NativeBridge.loadData(tempIndicesPtr, diskRowIds.toArray)
                
                // Blast the math natively!
                NativeBridge.avxUpdateMath(blockPtr, colIdx, opChar, operand, tempIndicesPtr, matchCount)
             } finally {
                NativeBridge.freeMainStore(tempIndicesPtr)
             }
          }
        }
      }

    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def scanFiltered(colName: String, opType: Int, targetVal: Int): Iterator[Array[Any]] = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Iterator.empty

    var ramIter: Iterator[Array[Any]] = Iterator.empty
    var snapshotBlocks: List[Long] = Nil

    rwLock.readLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")

      // 1. RAM Filter (Eager evaluation under lock)
      val ramRows = new scala.collection.mutable.ArrayBuffer[Array[Any]]()
      val colIntBuf = columns(colName).deltaIntBuffer
      var i = 0
      while (i < colIntBuf.length) {
         if (!ramDeleted.get(i)) {
           val v = colIntBuf(i)
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
                 if(col.isVector) row(c) = col.deltaVectorBuffer(i)
                 else if(col.isString) row(c) = col.deltaStringBuffer(i)
                 else row(c) = col.deltaIntBuffer(i)
                 c += 1
             }
             ramRows.append(row)
           }
         }
         i += 1
      }
      ramIter = ramRows.iterator

      // 2. Snapshot Native Blocks
      snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally {
      rwLock.readLock().unlock()
    }

    // 3. Disk Filter (Predicate Pushdown) safely protected by withEpoch
    // [PHASE 1 FIX] Change .iterator to .toList to force eager materialization!
    val diskList = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(blockPtr)
       val outIndicesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(rowCount * 4) // [FIX] Multiply by 4 for bytes
       
       val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
       
       try {
           val matchCount = org.awandb.core.jni.NativeBridge.avxFilterBlock(blockPtr, colIdx, opType, targetVal, outIndicesPtr, bitmaskPtr)
           
           if (matchCount == 0) {
               List.empty[Array[Any]]
           } else {
               // 1. First, pull the exact matched row indices into Scala
               val matchingIndices = new Array[Int](matchCount)
               org.awandb.core.jni.NativeBridge.copyToScala(outIndicesPtr, matchingIndices, matchCount)
               
               // 2. Allocate Scala arrays to hold the fully extracted columns
               val colsData = new Array[Array[Int]](columnOrder.size)
               
               for (c <- columnOrder.indices) {
                   val col = columns(columnOrder(c))
                   if (!col.isString && !col.isVector) { // Safe-guard against native string/vector corruption
                       colsData(c) = new Array[Int](matchCount)
                       val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, c)
                       val tempValuesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(matchCount * 4) // [FIX] Multiply by 4 for bytes
                       
                       org.awandb.core.jni.NativeBridge.batchRead(colPtr, outIndicesPtr, matchCount, tempValuesPtr)
                       org.awandb.core.jni.NativeBridge.copyToScala(tempValuesPtr, colsData(c), matchCount)
                       org.awandb.core.jni.NativeBridge.freeMainStore(tempValuesPtr)
                   }
               }
               
               // 4. Transpose the columnar arrays into rows purely in Scala (Zero JNI overhead)
               // [FIX] The scanFiltered Safety Patch! Do not map missing string/vector values to raw nulls.
               // [FIX 7] Do not map missing string/vector values to raw nulls.
               (0 until matchCount).map { i =>
                   val row = new Array[Any](columnOrder.size)
                   for (c <- columnOrder.indices) {
                       val col = columns(columnOrder(c))
                       if (col.isVector) row(c) = Array.empty[Float]
                       else if (col.isString) row(c) = "N/A (Disk)"
                       else row(c) = if (colsData(c) != null) colsData(c)(i) else null
                   }
                   row
               }.toList
           }
       } finally {
           org.awandb.core.jni.NativeBridge.freeMainStore(outIndicesPtr)
           // Do NOT free bitmaskPtr here! BlockManager owns it.
       }
    }.toList // Force execution inside the EBMM boundary

    ramIter ++ diskList.iterator
  }

  /**
   * Late Materialization Filter
   * Scans a column and returns ONLY the Primary Keys (Row IDs) that match.
   */
  def scanFilteredIds(colName: String, opType: Int, targetVal: Int): Array[Int] = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Array.empty[Int]
    
    val idColName = columnOrder.head // Assumes Column 0 is the PK

    val ramIds = new scala.collection.mutable.ArrayBuffer[Int]()
    var snapshotBlocks: List[Long] = Nil

    rwLock.readLock().lock()
    try {
       if (isClosed) throw new IllegalStateException("Table is closed")

       // 1. RAM Filter (Primitive Array Loop)
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

       // 2. Snapshot Native Blocks
       snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally {
       rwLock.readLock().unlock()
    }

    // 3. Disk Filter (Predicate Pushdown) safely protected by withEpoch
    val diskIds = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = NativeBridge.getRowCount(blockPtr)
       val outIndicesPtr = NativeBridge.allocMainStore(rowCount * 4) // [FIX] Multiply by 4 for bytes
       val bitmaskPtr = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
       
       try {
           val matchCount = NativeBridge.avxFilterBlock(blockPtr, colIdx, opType, targetVal, outIndicesPtr, bitmaskPtr)
           
           if (matchCount == 0) {
               Array.empty[Int]
           } else {
               // ONLY Gather Column 0 (The Primary Key)
               val idColPtr = NativeBridge.getColumnPtr(blockPtr, 0)
               val tempValuesPtr = NativeBridge.allocMainStore(matchCount * 4) // [FIX] Multiply by 4 for bytes
               
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
      // 1. Gather all pointers to retire BEFORE swapping (so we can find indices for filters)
      oldBlocks.foreach { ptr =>
        epochManager.retire(ptr, 1) // [FIX] resourceType 1 = Block Object
        
        // Retire the native bitmasks to prevent memory leaks (these are raw memory, not blocks)
        val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
        val bitmaskPtr = blockManager.getNativeDeletionBitmap(ptr, rowCount)
        if (bitmaskPtr != 0L) epochManager.retire(bitmaskPtr, 0) // [FIX] resourceType 0 = Raw Memory
        
        // [UPDATED] Find the block index to retire ALL its Cuckoo Filters!
        val blockIdx = blockManager.getLoadedBlocks.indexOf(ptr)
        if (blockIdx != -1) {
          val filterArray = blockManager.getFilterArray(blockIdx)
          if (filterArray != null) {
            filterArray.foreach { filterPtr =>
               if (filterPtr != 0L) {
                 // [CRITICAL FIX] Route Cuckoo Filters to their explicit C++ destructor 
                 // to prevent native heap corruption!
                 epochManager.retire(filterPtr, 2) // [FIX] resourceType 2 = Cuckoo Filter Object
               }
            }
          }
        }
      }

      // 2. Swap the blocks safely inside BlockManager
      blockManager.swapBlocks(oldBlocks, newBlockPtr)
      
      // 3. Rebuild the Primary Index for ALL rows to fix shifted block indices
      primaryIndex.clear()
      rebuildPrimaryIndex()
      
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def queryVector(colName: String, query: Array[Float], threshold: Float, limit: Int = 100): Array[Int] = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Array.empty[Int]

    // 1. Generate a unique Query ID for this execution's C++ Memory Arena
    val queryId = java.util.UUID.randomUUID().toString
    
    // 2. Initialize the C++ Query Context
    org.awandb.core.jni.NativeBridge.initQueryContext(queryId)

    try {
      var snapshotBlocks: List[Long] = Nil
      val ramMatches = scala.collection.mutable.ArrayBuffer[(Int, Float)]()

      // 1. Lock the table for reading
      rwLock.readLock().lock()
      try {
        if (isClosed) throw new IllegalStateException("Table is closed")

        // --- RAM VECTOR SEARCH ---
        val col = columns(colName)
        if (col.isVector && col.deltaVectorBuffer.nonEmpty) {
          val ramVectors = col.deltaVectorBuffer
          val ramIdCol = columns(columnOrder.head).deltaIntBuffer // Assumes Col 0 is PK
          
          var i = 0
          while (i < ramVectors.length) {
            if (!ramDeleted.get(i)) {
               val vec = ramVectors(i)
               if (vec != null && vec.length == query.length) {
                   var dotProduct = 0.0f
                   var normA = 0.0f
                   var normB = 0.0f
                   var d = 0
                   
                   // Compute Cosine Similarity strictly for vectors in RAM
                   while (d < vec.length) {
                      dotProduct += vec(d) * query(d)
                      normA += vec(d) * vec(d)
                      normB += query(d) * query(d)
                      d += 1
                   }
                   if (normA > 0 && normB > 0) {
                       val score = dotProduct / (math.sqrt(normA) * math.sqrt(normB)).toFloat
                       if (score >= threshold) {
                           ramMatches.append((ramIdCol(i), score))
                       }
                   }
               }
            }
            i += 1
          }
        }

        snapshotBlocks = blockManager.getLoadedBlocks.toList

      } finally {
        rwLock.readLock().unlock()
      }
      
      // 2. DISK VECTOR SEARCH (JNI)
      // Protected safely by the outer withEpoch wrapper!
      val diskMatches = snapshotBlocks.flatMap { blockPtr =>
         val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(blockPtr)
         
         val outIndicesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(rowCount * 4)
         val outScoresPtr  = org.awandb.core.jni.NativeBridge.allocMainStore(rowCount * 4) 
         
         try {
             val matchCount = org.awandb.core.jni.NativeBridge.avxScanVectorCosine(
               blockPtr, colIdx, query, threshold, outIndicesPtr, outScoresPtr
             )
             
             if (matchCount == 0) {
                 Array.empty[(Int, Float)] 
             } else {
                 val idColPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, 0)
                 val tempValuesPtr = org.awandb.core.jni.NativeBridge.allocMainStore(matchCount * 4)
                 org.awandb.core.jni.NativeBridge.batchRead(idColPtr, outIndicesPtr, matchCount, tempValuesPtr)
                 
                 val ids = new Array[Int](matchCount)
                 org.awandb.core.jni.NativeBridge.copyToScala(tempValuesPtr, ids, matchCount)
                 org.awandb.core.jni.NativeBridge.freeMainStore(tempValuesPtr)
                 
                 val scores = new Array[Float](matchCount)
                 org.awandb.core.jni.NativeBridge.copyToScalaFloat(outScoresPtr, scores, matchCount) 
                 
                 ids.zip(scores)
             }
         } finally {
             org.awandb.core.jni.NativeBridge.freeMainStore(outIndicesPtr)
             org.awandb.core.jni.NativeBridge.freeMainStore(outScoresPtr)
         }
      }
      
      // 3. Late-Stage Filtering & Top-K Ranking (Merging RAM and Disk)
      val allMatches = ramMatches ++ diskMatches

      allMatches
        .filter { case (id, _) => getRow(id).isDefined } 
        .distinctBy { case (id, _) => id }               
        .sortBy { case (_, score) => -score }            
        .take(limit)                                     
        .map { case (id, _) => id }                      
        .toArray

    } finally {
      // 4. CRITICAL: Destroy the C++ arena, guaranteed to run even if an exception occurs
      org.awandb.core.jni.NativeBridge.destroyQueryContext(queryId)
    }
  }

  // ---------------------------------------------------------
  // GRAPH PROJECTION
  // ---------------------------------------------------------
  
  def projectToGraph(srcColName: String, dstColName: String): org.awandb.core.graph.GraphTable = withEpoch {
    val srcIdx = columnOrder.indexOf(srcColName)
    val dstIdx = columnOrder.indexOf(dstColName)
    
    if (srcIdx == -1 || dstIdx == -1) {
        throw new IllegalArgumentException("Source or Destination column not found for Graph Projection.")
    }

    // 1. Gather all active edges (Reads from both RAM and Disk, automatically skipping tombstones)
    val edges = scala.collection.mutable.ArrayBuffer[(Int, Int)]()
    val it = scanAll() // scanAll lazy iterator is safely consumed inside withEpoch
    
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

  // ---------------------------------------------------------
  // AGGREGATION PUSHDOWNS (Zero-Allocation)
  // ---------------------------------------------------------
  
  def countAll(): Long = withEpoch {
    var count = 0L
    var snapshotBlocks: List[Long] = Nil
    
    rwLock.readLock().lock()
    try {
        if (columns.nonEmpty) {
            count += columns.values.head.deltaIntBuffer.length - ramDeleted.cardinality()
        }
        snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally { rwLock.readLock().unlock() }
    
    snapshotBlocks.foreach { ptr =>
       val total = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
       val delBitset = blockManager.getDeletionBitSet(ptr)
       val delCount = if (delBitset == null) 0 else delBitset.cardinality()
       count += (total - delCount)
    }
    count
  }

  def sumColumn(colName: String): Long = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return 0L
    var sum = 0L
    var snapshotBlocks: List[Long] = Nil
    
    rwLock.readLock().lock()
    try {
       val colData = columns(colName).deltaIntBuffer
       var i = 0
       while (i < colData.length) {
          if (!ramDeleted.get(i)) sum += colData(i)
          i += 1
       }
       snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally { rwLock.readLock().unlock() }
    
    snapshotBlocks.foreach { ptr =>
       val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
       val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(ptr, colIdx)
       val data = new Array[Int](rowCount)
       org.awandb.core.jni.NativeBridge.copyToScala(colPtr, data, rowCount)
       
       val isClean = blockManager.isClean(ptr)
       var i = 0
       if (isClean) {
           while(i < rowCount) { sum += data(i); i += 1 }
       } else {
           while(i < rowCount) {
               if (!blockManager.isDeleted(ptr, i)) sum += data(i)
               i += 1
           }
       }
    }
    sum
  }

  def sumFilteredIds(colName: String, matchedIds: Array[Int]): Long = withEpoch {
     val colIdx = columnOrder.indexOf(colName)
     if (colIdx == -1) return 0L
     var sum = 0L
     
     rwLock.readLock().lock()
     try {
         val tmp = new Array[Int](1)
         var i = 0
         while (i < matchedIds.length) {
            val loc = primaryIndex.get(matchedIds(i))
            if (loc != null) {
                val bIdx = unpackBlockIdx(loc)
                val rId = unpackRowId(loc)
                if (bIdx == -1) {
                    sum += columns(colName).deltaIntBuffer(rId)
                } else {
                    val bPtr = blockManager.getBlockPtr(bIdx)
                    val cPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(bPtr, colIdx)
                    val cellPtr = org.awandb.core.jni.NativeBridge.getOffsetPointer(cPtr, rId * 4L)
                    org.awandb.core.jni.NativeBridge.copyToScala(cellPtr, tmp, 1)
                    sum += tmp(0)
                }
            }
            i += 1
         }
     } finally { rwLock.readLock().unlock() }
     sum
  }

  def maxColumn(colName: String): Int = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return 0
    var max = Int.MinValue
    var snapshotBlocks: List[Long] = Nil
    
    rwLock.readLock().lock()
    try {
       val colData = columns(colName).deltaIntBuffer
       var i = 0
       while (i < colData.length) {
          if (!ramDeleted.get(i)) {
              if (colData(i) > max) max = colData(i)
          }
          i += 1
       }
       snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally { rwLock.readLock().unlock() }
    
    snapshotBlocks.foreach { ptr =>
       val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
       val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(ptr, colIdx)
       val data = new Array[Int](rowCount)
       org.awandb.core.jni.NativeBridge.copyToScala(colPtr, data, rowCount)
       
       val isClean = blockManager.isClean(ptr)
       var i = 0
       if (isClean) {
           while(i < rowCount) { if (data(i) > max) max = data(i); i += 1 }
       } else {
           while(i < rowCount) {
               if (!blockManager.isDeleted(ptr, i)) {
                   if (data(i) > max) max = data(i)
               }
               i += 1
           }
       }
    }
    if (max == Int.MinValue && countAll() == 0) 0 else max
  }

  def minColumn(colName: String): Int = withEpoch {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return 0
    var min = Int.MaxValue
    var snapshotBlocks: List[Long] = Nil
    
    rwLock.readLock().lock()
    try {
       val colData = columns(colName).deltaIntBuffer
       var i = 0
       while (i < colData.length) {
          if (!ramDeleted.get(i)) {
              if (colData(i) < min) min = colData(i)
          }
          i += 1
       }
       snapshotBlocks = blockManager.getLoadedBlocks.toList
    } finally { rwLock.readLock().unlock() }
    
    snapshotBlocks.foreach { ptr =>
       val rowCount = org.awandb.core.jni.NativeBridge.getRowCount(ptr)
       val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(ptr, colIdx)
       val data = new Array[Int](rowCount)
       org.awandb.core.jni.NativeBridge.copyToScala(colPtr, data, rowCount)
       
       val isClean = blockManager.isClean(ptr)
       var i = 0
       if (isClean) {
           while(i < rowCount) { if (data(i) < min) min = data(i); i += 1 }
       } else {
           while(i < rowCount) {
               if (!blockManager.isDeleted(ptr, i)) {
                   if (data(i) < min) min = data(i)
               }
               i += 1
           }
       }
    }
    if (min == Int.MaxValue && countAll() == 0) 0 else min
  }

  def maxFilteredIds(colName: String, matchedIds: Array[Int]): Int = withEpoch {
     val colIdx = columnOrder.indexOf(colName)
     if (colIdx == -1) return 0
     var max = Int.MinValue
     
     rwLock.readLock().lock()
     try {
         val tmp = new Array[Int](1)
         var i = 0
         while (i < matchedIds.length) {
            val loc = primaryIndex.get(matchedIds(i))
            if (loc != null) {
                val bIdx = unpackBlockIdx(loc)
                val rId = unpackRowId(loc)
                val v = if (bIdx == -1) {
                    columns(colName).deltaIntBuffer(rId)
                } else {
                    val bPtr = blockManager.getBlockPtr(bIdx)
                    val cPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(bPtr, colIdx)
                    val cellPtr = org.awandb.core.jni.NativeBridge.getOffsetPointer(cPtr, rId * 4L)
                    org.awandb.core.jni.NativeBridge.copyToScala(cellPtr, tmp, 1)
                    tmp(0)
                }
                if (v > max) max = v
            }
            i += 1
         }
     } finally { rwLock.readLock().unlock() }
     if (max == Int.MinValue && matchedIds.length == 0) 0 else max
  }

  def minFilteredIds(colName: String, matchedIds: Array[Int]): Int = withEpoch {
     val colIdx = columnOrder.indexOf(colName)
     if (colIdx == -1) return 0
     var min = Int.MaxValue
     
     rwLock.readLock().lock()
     try {
         val tmp = new Array[Int](1)
         var i = 0
         while (i < matchedIds.length) {
            val loc = primaryIndex.get(matchedIds(i))
            if (loc != null) {
                val bIdx = unpackBlockIdx(loc)
                val rId = unpackRowId(loc)
                val v = if (bIdx == -1) {
                    columns(colName).deltaIntBuffer(rId)
                } else {
                    val bPtr = blockManager.getBlockPtr(bIdx)
                    val cPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(bPtr, colIdx)
                    val cellPtr = org.awandb.core.jni.NativeBridge.getOffsetPointer(cPtr, rId * 4L)
                    org.awandb.core.jni.NativeBridge.copyToScala(cellPtr, tmp, 1)
                    tmp(0)
                }
                if (v < min) min = v
            }
            i += 1
         }
     } finally { rwLock.readLock().unlock() }
     if (min == Int.MaxValue && matchedIds.length == 0) 0 else min
  }

  // ---------------------------------------------------------
  // LIFECYCLE MANAGEMENT (CLOSE)
  // ---------------------------------------------------------
  def close(): Unit = {
    // 1. Mark as closed to prevent new operations
    rwLock.writeLock().lock()
    try {
      if (isClosed) return
      isClosed = true
    } finally {
      rwLock.writeLock().unlock()
    }
    
    // 2. Halt background processing immediately
    if (daemonThread != null) daemonThread.interrupt() 
    if (engineManager != null) engineManager.stopEngine()
    
    // 3. Wait for active JNI/Background sweeps to safely exit
    try { if (daemonThread != null) daemonThread.join() } catch { case _: Exception => }
    try { if (engineManager != null) engineManager.joinThread() } catch { case _: Exception => }
    
    // 4. Safely deallocate native memory and clear data structures
    rwLock.writeLock().lock()
    try {
      // Untrack data blocks before closing the BlockManager
      if (blockManager != null) {
        blockManager.getLoadedBlocks.foreach { ptr =>
            org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(ptr)
        }
        blockManager.close()
      }
      
      if (wal != null) wal.close()
      
      // This properly delegates column cleanup (buffers, dictionaries) to NativeColumn
      if (columns != null) columns.values.foreach(_.close()) 

      // Clean up Result Index Buffer safely
      if (resultIndexBuffer != 0L) {
          org.awandb.core.jni.NativeBridge.freeMainStore(resultIndexBuffer)
          org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(resultIndexBuffer)
          resultIndexBuffer = 0L // Reset pointer (Requires resultIndexBuffer to be a var)
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // ZERO-LEAK DDL (DROP TABLE)
  // ---------------------------------------------------------
  def drop(): Unit = {
    // 1. Signal threads to stop immediately
    isClosed = true 
    if (daemonThread != null && daemonThread.isAlive) {
      daemonThread.interrupt()
    }
    if (engineManager != null) {
      engineManager.stopEngine()
    }

    // 2. Instant File System Rename (0ms Latency)
    // Move the data directory out of the way so new tables with the same name can be created instantly.
    val currentDir = new java.io.File(dataDir)
    val trashDir = new java.io.File(s"${currentDir.getParent}/trash/${name}_${System.currentTimeMillis()}")
    trashDir.getParentFile.mkdirs()
    
    // [CRITICAL FIX] Capture the rename result. On Windows, open mmap handles can block this.
    val renameSuccess = currentDir.renameTo(trashDir)

    // 3. Spin off the Detached Drop Thread for heavy lifting
    val dropThread = new Thread(new Runnable {
      override def run(): Unit = {
        // A. WAIT for background threads to safely exit the C++ boundary!
        // If we free memory while they are in C++, the JVM will instantly Segfault.
        try { if (daemonThread != null) daemonThread.join() } catch { case _: Exception => }
        try { if (engineManager != null) engineManager.joinThread() } catch { case _: Exception => }

        rwLock.writeLock().lock()
        try {
          // B. Let columns clean up their own buffers AND Native C++ Dictionaries
          columns.values.foreach(_.close())
          columns.clear()
          columnOrder.clear()
          ramDeleted.clear()
          primaryIndex.clear()
          vectorDims.clear()

          // C. EXPLICIT NATIVE MEMORY WIPEOUT
          if (blockManager != null) {
            blockManager.getLoadedBlocks.foreach { ptr =>
                org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(ptr)
            }
            blockManager.dropAllBlocksInstantly() 
          }
          
          if (wal != null) {
            wal.drop() 
          }

          // D. Safely Free and Zero the Main Store Buffer to prevent Double-Free
          if (resultIndexBuffer != 0L) {
            org.awandb.core.jni.NativeBridge.freeMainStore(resultIndexBuffer)
            org.awandb.core.engine.memory.NativeMemoryTracker.recordDeallocation(resultIndexBuffer)
            resultIndexBuffer = 0L 
          }

          // E. Execute STRIKE 1 FIX: Nuke all orphaned native memory
          epochManager.forceReclaimAll()

          // F. Safely wipe the directory from the disk in the background
          def deleteRecursively(f: java.io.File): Unit = {
            if (f.isDirectory) {
              val children = f.listFiles()
              if (children != null) children.foreach(deleteRecursively)
            }
            // Windows File-Lock Defeater
            if (f.exists() && !f.delete()) {
                System.gc() // Force JVM to release any lingering streams
                Thread.sleep(50) // Give the OS a millisecond to catch up
                f.delete() // Try again!
            }
          }
          
          // [CRITICAL FIX] If rename failed due to native locks, delete the original dir!
          if (renameSuccess) deleteRecursively(trashDir)
          else deleteRecursively(currentDir)

          // [CRITICAL FIX] Force the OS to instantly reclaim the dropped table's memory!
          org.awandb.core.jni.NativeBridge.trimMemory()

        } finally {
          rwLock.writeLock().unlock()
        }
      }
    }, s"AwanDB-DropThread-$name")

    dropThread.setDaemon(true)
    dropThread.start()
  }

  def persist(dir: String): Unit = flush() 
  def restore(dir: String, count: Int): Unit = {} 
  def selectWhere(targetCol: String, filterCol: String, threshold: Int): Array[Int] = Array.empty[Int]
}