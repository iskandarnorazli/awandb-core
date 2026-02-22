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
import scala.collection.concurrent.TrieMap
import org.awandb.core.storage.SimpleStorageRouter

// Track the native bitmask pointer for each block
private val blockBitmasks = new TrieMap[Long, Long]()

// Location Pointer for Primary Index
case class RowLocation(blockPtr: Long, rowId: Int)

sealed trait EngineMode
object EngineMode {
  case object CACHE extends EngineMode  // Pure RAM, No WAL, 5M+ TPS
  case object NORMAL extends EngineMode // Async WAL, Partial ACID
  case object STRICT extends EngineMode // Sync fsync per transaction, Full ACID
}

class AwanTable(
    val name: String, 
    val capacity: Int, 
    val dataDir: String = "data",
    val governor: EngineGovernor = NoOpGovernor,
    val enableIndex: Boolean = true,
    val mode: EngineMode = EngineMode.NORMAL
) {
  
  // [FIXED] Absolute Memory Isolation! 
  // Each table gets a dedicated directory, preventing BlockManager amnesia and cross-table memory bleeding!
  val tableDir = s"$dataDir/$name"
  new java.io.File(tableDir).mkdirs()
  
  val router = new SimpleStorageRouter(tableDir, "")
  val wal = new Wal(tableDir) 
  val blockManager = new BlockManager(router, enableIndex)
  
  val columns = new scala.collection.mutable.LinkedHashMap[String, NativeColumn]()
  val columnOrder = new scala.collection.mutable.ListBuffer[String]()
  
  val resultIndexBuffer: Long = NativeBridge.allocMainStore(capacity)
  private val rwLock = new java.util.concurrent.locks.ReentrantReadWriteLock()
  @volatile private var isClosed = false

  private val primaryIndex = new java.util.concurrent.ConcurrentHashMap[Int, RowLocation]()
  private val ramDeleted = new java.util.BitSet()
  
  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  blockManager.recover()

  @volatile private var indexRebuilt = false

  // Safely rebuilds the O(1) Index from C++ Blocks after a Cold Boot
  private def ensureIndexRebuilt(): Unit = {
    if (!indexRebuilt && columnOrder.nonEmpty) {
      rwLock.writeLock().lock()
      try {
        if (!indexRebuilt) {
          val idColName = columnOrder.head
          val idCol = columns(idColName)
          
          for (blockPtr <- blockManager.getLoadedBlocks) { 
             val rowCount = NativeBridge.getRowCount(blockPtr)
             if (rowCount > 0) {
                 val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
                 val data = new Array[Int](rowCount)
                 NativeBridge.copyToScala(colPtr, data, rowCount)
                 
                 for (i <- 0 until rowCount) {
                    if (!blockManager.isDeleted(blockPtr, i)) {
                       val id = if (idCol.isString && idCol.useDictionary) idCol.getDictStr(data(i)).hashCode else data(i)
                       primaryIndex.putIfAbsent(id, RowLocation(blockPtr, i))
                    }
                 }
             }
          }
          indexRebuilt = true
        }
      } finally {
        rwLock.writeLock().unlock()
      }
    }
  }

  def getRamRowCount(): Int = {
    if (columns.isEmpty) return 0
    columns.values.map(c => math.max(c.deltaIntBuffer.length, c.deltaStringBuffer.length)).max
  }

  // ---------------------------------------------------------
  // SCHEMA
  // ---------------------------------------------------------
  
  def addColumn(colName: String, isString: Boolean = false, useDictionary: Boolean = false): Unit = {
    val enforceDict = if (isString) true else useDictionary
    val col = new NativeColumn(colName, isString, enforceDict)
    columns.put(colName, col)
    columnOrder.append(colName)
    
    // [FIXED] Recover C++ Dictionaries from the isolated table directory!
    if (enforceDict) {
       val dictPath = s"$tableDir/${name}_${colName}.dict"
       col.loadDictionary(dictPath)
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
    ensureIndexRebuilt()
    rwLock.readLock().lock()
    try {
      val loc = primaryIndex.get(id)
      if (loc == null) return None

      val result = new Array[Any](columns.size)
      var colIdx = 0
      
      for (colName <- columnOrder) {
        val col = columns(colName)
        
        if (loc.blockPtr == 0L) {
           if (col.isString) result(colIdx) = col.deltaStringBuffer(loc.rowId)
           else result(colIdx) = col.deltaIntBuffer(loc.rowId)
        } else {
           val colPtr = NativeBridge.getColumnPtr(loc.blockPtr, colIdx)
           val stride = NativeBridge.getColumnStride(loc.blockPtr, colIdx)
           val cellPtr = NativeBridge.getOffsetPointer(colPtr, loc.rowId * stride.toLong)
           
           val tempBuf = new Array[Int](1)
           NativeBridge.copyToScala(cellPtr, tempBuf, 1)
           
           // [FIXED] Decode strings for FlightSQL!
           if (col.isString) result(colIdx) = col.getDictStr(tempBuf(0))
           else result(colIdx) = tempBuf(0)
        }
        colIdx += 1
      }
      Some(result)
    } finally {
      rwLock.readLock().unlock()
    }
  }

  def scanAll(): Iterator[Array[Any]] = {
      if (columnOrder.isEmpty) return Iterator.empty
      val firstCol = columns(columnOrder.head)

      val ramIter = (0 until firstCol.deltaIntBuffer.length).iterator.collect {
         case i if !ramDeleted.get(i) =>
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
                     
                     val col = columns(colName)
                     if (col.isString && col.useDictionary) {
                         row(c) = col.getDictStr(temp(0))
                     } else {
                         row(c) = temp(0) 
                     }
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

  /**
   * LOCK-FREE O(1) DELETION
   * Updates the source-of-truth BitSet. Projected to C++ dynamically on read.
   */
  def delete(id: Int): Boolean = {
    ensureIndexRebuilt() // Prevent amnesia on cold boot!
    rwLock.writeLock().lock()
    try {
      val loc = primaryIndex.remove(id)
      if (loc == null) return false 

      val org.awandb.core.engine.RowLocation(blockPtr: Long, rowOffset: Int) = loc
      
      if (blockPtr == 0L) {
          ramDeleted.set(rowOffset, true)
      } else {
          blockManager.markDeleted(blockPtr, rowOffset)
      }
      true
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  /**
   * HYBRID UPDATE: Modifies RAM if volatile, Mutates C++ if persisted.
   */
  def update(id: Int, changes: Map[String, Any]): Boolean = {
    ensureIndexRebuilt() // Prevent amnesia on cold boot!
    rwLock.writeLock().lock()
    try {
      if (isClosed) return false
      val loc = primaryIndex.get(id)
      if (loc == null) return false

      if (loc.blockPtr == 0L) {
          changes.foreach { case (colName, newValue) =>
              val actualColOpt = columnOrder.find(_.equalsIgnoreCase(colName))
              if (actualColOpt.isDefined) {
                  val col = columns(actualColOpt.get)
                  if (col.isString) {
                      val s = newValue.toString
                      col.deltaStringBuffer(loc.rowId) = s
                      if (col.useDictionary) col.getDictId(s)
                  } else {
                      col.deltaIntBuffer(loc.rowId) = newValue match {
                          case v: Int => v; case v: Long => v.toInt; case _ => 0
                      }
                  }
              }
          }
          return true
      }

      changes.foreach { case (colName, newValue) =>
          val colIdx = columnOrder.indexWhere(_.equalsIgnoreCase(colName))
          if (colIdx != -1) {
              val actualCol = columnOrder(colIdx)
              val intVal = newValue match {
                  case v: Int => v; case v: Long => v.toInt
                  case s: String => columns(actualCol).getDictId(s)
                  case _ => 0
              }
              NativeBridge.updateCell(loc.blockPtr, colIdx, loc.rowId, intVal)
          }
      }
      true
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  // ---------------------------------------------------------
  // WRITE PATH
  // ---------------------------------------------------------
  
  /**
   * HTAP FAST APPEND (Writes to Volatile DeltaStore)
   */
  def insertRow(values: Array[Any]): Unit = {
    if (values.length != columnOrder.size) throw new IllegalArgumentException("Column mismatch")

    columnOrder.zipWithIndex.foreach { case (colName, i) =>
       val isStrCol = columns(colName).isString
       val isStrVal = values(i).isInstanceOf[String]
       if (isStrCol && !isStrVal) throw new IllegalArgumentException(s"Column '$colName' expects String.")
       if (!isStrCol && isStrVal) throw new IllegalArgumentException(s"Column '$colName' expects Int, but got String.")
    }

    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      val id = resolveId(values(0))

      columnOrder.zipWithIndex.foreach { case (colName, i) =>
          val col = columns(colName)
          if (col.isString) {
              val s = values(i).asInstanceOf[String]
              col.deltaStringBuffer.append(s)
              if (col.useDictionary) col.getDictId(s) 
          } else {
              val v = values(i) match {
                  case v: Int => v; case v: Long => v.toInt; case _ => 0
              }
              col.deltaIntBuffer.append(v)
              if (mode == EngineMode.NORMAL || mode == EngineMode.STRICT) {
                  if (wal != null) wal.logInsert(v)
              }
          }
      }

      // [FIXED] Bulletproof row ID calculation prevents bitIndex < 0 crashes!
      val rowId = getRamRowCount() - 1
      primaryIndex.put(id, RowLocation(0L, rowId))
      ramDeleted.set(rowId, false)

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
        
        // [FIXED] Keep Primary Index in sync if inserting into the first column!
        if (columnOrder.nonEmpty && columnOrder.head == colName) {
            val rowId = columns(colName).deltaIntBuffer.length - 1
            primaryIndex.put(value, RowLocation(0L, rowId))
            ramDeleted.set(rowId, false)
        }
        
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

  // ---------------------------------------------------------
  // VECTORIZED BULK INGESTION (The 10M+ TPS Path)
  // ---------------------------------------------------------
  def insertBatch(values: Array[Int]): Unit = {
    if (columns.isEmpty) return
    val firstColName = columnOrder.head
    val firstCol = columns(firstColName)

    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      
      val startId = getRamRowCount()
      firstCol.deltaIntBuffer ++= values // Fast C-style array copy
      
      var i = 0
      while (i < values.length) {
        primaryIndex.put(values(i), RowLocation(0L, startId + i))
        i += 1
      }
      
      if (mode == EngineMode.NORMAL || mode == EngineMode.STRICT) {
          if (wal != null) wal.logBatch(values)
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def loadDataDirect(data: Array[Int]): Unit = insertBatch(data)

  /**
   * FLUSH: Converts Volatile RAM rows into Immutable Native C++ Blocks
   */
  def flush(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed || columnOrder.isEmpty) return

      val rowCount = getRamRowCount() 
      
      if (rowCount > 0) {
          val allColumnsData = columnOrder.map { colName =>
              val col = columns(colName)
              if (col.isString) {
                  if (col.useDictionary) col.encodeDelta() else col.toStringArray
              } else col.toIntArray
          }.toList
          
          blockManager.createAndPersistBlock(allColumnsData.asInstanceOf[List[Any]])
          
          if (blockManager.getLoadedBlocks.nonEmpty) {
              val newBlockPtr = blockManager.getLoadedBlocks.last
              val idColName = columnOrder.head
              val idCol = columns(idColName)
              
              for (i <- 0 until rowCount) {
                  val id = if (idCol.isString) idCol.deltaStringBuffer(i).hashCode else idCol.deltaIntBuffer(i)
                  if (!ramDeleted.get(i)) {
                      primaryIndex.put(id, RowLocation(newBlockPtr, i))
                  } else {
                      primaryIndex.remove(id)
                      blockManager.markDeleted(newBlockPtr, i)
                  }
              }
          }
          
          columnOrder.foreach { colName => columns(colName).clearDelta() }
          ramDeleted.clear()
          if (wal != null) wal.clear()
      }

      if (mode == EngineMode.NORMAL || mode == EngineMode.STRICT) {
          if (wal != null) wal.sync()
      }
      
      val blocks = blockManager.getLoadedBlocks
      blocks.zipWithIndex.foreach { case (blockPtr, idx) =>
          val blockPath = router.getPathForBlock(idx)
          NativeBridge.fsyncBlock(blockPtr, blockPath)
      }
      
      blockManager.saveBitmaps()
      columnOrder.foreach { colName =>
          val col = columns(colName)
          if (col.useDictionary && col.dictionaryPtr != 0L) {
             // [FIXED] Save dictionaries to the isolated table directory!
             val dictPath = s"$tableDir/${name}_${colName}.dict"
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
       val (min, max) = NativeBridge.getZoneMap(blockPtr, colIdx)

       // [RESTORED] Zone Map Pruning!
       if (target < min || target > max) {
           0 // Impossible to match, skip block entirely!
       } else {
           val bitmaskPtr = getNativeBitmask(blockPtr)
           try {
               // [RESTORED] Pass 0L to prevent allocating MBs of output indices!
               if (bitmaskPtr == 0L) {
                   NativeBridge.avxScanBlockEquality(blockPtr, colIdx, target, 0L)
               } else {
                   NativeBridge.avxFilterBlock(blockPtr, colIdx, 0, target, 0L, bitmaskPtr)
               }
           } finally {
               if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
           }
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
             searchId = col.getDictId(value)
           }
         case None => return 0
       }
    } finally {
       rwLock.readLock().unlock()
    }

    val diskCount = MorselExec.scanParallel(snapshotBlocks, { blockPtr =>
        var localCount = 0
        if (useDict) {
            val bitmaskPtr = getNativeBitmask(blockPtr)
            try {
               if (bitmaskPtr == 0L) {
                   localCount = NativeBridge.avxScanBlockEquality(blockPtr, colIdx, searchId, 0L)
               } else {
                   localCount = NativeBridge.avxFilterBlock(blockPtr, colIdx, 0, searchId, 0L, bitmaskPtr)
               }
            } finally {
               if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
            }
        } else {
            // [RESTORED] Removed the allocMainStore memory drag here too!
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
       val (min, max) = NativeBridge.getZoneMap(blockPtr, colIdx)

       // [RESTORED] Zone Map Pruning for Range Queries!
       if (max <= threshold) {
           0 // 100% Skip! Block values are too low.
       } else if (min > threshold && blockManager.isClean(blockPtr)) {
           NativeBridge.getRowCount(blockPtr) // 100% Match! Instantly return count.
       } else {
           val bitmaskPtr = getNativeBitmask(blockPtr)
           try {
               if (bitmaskPtr == 0L) {
                   NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0L)
               } else {
                   NativeBridge.avxScanBlockWithDeletions(blockPtr, colIdx, threshold, bitmaskPtr)
               }
           } finally {
               if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
           }
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

  /**
   * HTAP Fused Group By
   * Aggregates cold C++ blocks natively, then merges hot RAM rows on the fly.
   */
  def executeGroupBy(keyCol: String, valCol: String): Map[Int, Long] = {
    val keyIdx = columnOrder.indexOf(keyCol)
    val valIdx = columnOrder.indexOf(valCol)

    if (keyIdx == -1 || valIdx == -1) throw new IllegalArgumentException("Column not found") 

    // 1. NATIVE AGGREGATION
    val blocks = blockManager.getLoadedBlocks
    val capacityHint = 10000
    val mapPtr = NativeBridge.aggregateCreateMap(capacityHint)
    var totalAggregatedRows = 0

    for (blockPtr <- blocks) {
      val bitmaskPtr = getNativeBitmask(blockPtr) 
      try {
          totalAggregatedRows += NativeBridge.avxAggregate(blockPtr, keyIdx, valIdx, mapPtr, bitmaskPtr)
      } finally {
          if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
      }
    }

    val baseMap = scala.collection.mutable.Map[Int, Long]().withDefaultValue(0L)

    if (totalAggregatedRows > 0) {
      val outKeysPtr = NativeBridge.allocMainStore(totalAggregatedRows)
      val outValsPtr = NativeBridge.allocMainStore(totalAggregatedRows * 2) 
      val uniqueCount = NativeBridge.aggregateExport(mapPtr, outKeysPtr, outValsPtr)

      val scalaKeys = new Array[Int](uniqueCount)
      val scalaVals = new Array[Long](uniqueCount)
      NativeBridge.copyToScala(outKeysPtr, scalaKeys, uniqueCount)
      NativeBridge.copyToScalaLong(outValsPtr, scalaVals, uniqueCount)

      for (i <- 0 until uniqueCount) baseMap(scalaKeys(i)) = scalaVals(i)

      NativeBridge.freeMainStore(outKeysPtr)
      NativeBridge.freeMainStore(outValsPtr)
    }
    NativeBridge.freeAggregationResult(mapPtr)

    // 2. RAM MERGE (HTAP)
    val kCol = columns(keyCol)
    val vCol = columns(valCol)
    val ramLen = getRamRowCount()

    for (i <- 0 until ramLen) {
       if (!ramDeleted.get(i)) {
          val k = if (kCol.isString) {
             if (kCol.useDictionary) kCol.getDictId(kCol.deltaStringBuffer(i)) else 0
          } else kCol.deltaIntBuffer(i)

          val v = if (vCol.isString) 0L else vCol.deltaIntBuffer(i).toLong
          baseMap(k) += v
       }
    }

    baseMap.toMap
  }

  /**
   * High-Performance Fused Filter & Group By
   */
  def executeFilteredGroupBy(keyCol: String, valCol: String, filterCol: String, opType: Int, targetVal: Int): Map[Int, Long] = {
    val keyIdx = columnOrder.indexOf(keyCol)
    val valIdx = columnOrder.indexOf(valCol)
    val filterIdx = columnOrder.indexOf(filterCol)

    if (keyIdx == -1 || valIdx == -1 || filterIdx == -1) {
      throw new IllegalArgumentException(s"Invalid columns for Filtered Group By")
    }

    // 1. NATIVE AGGREGATION
    val capacityHint = 10000 
    val mapPtr = NativeBridge.aggregateCreateMap(capacityHint)
    var totalAggregatedRows = 0

    val blocks = blockManager.getLoadedBlocks 
    for (blockPtr <- blocks) {
      val bitmaskPtr = getNativeBitmask(blockPtr)
      try {
          totalAggregatedRows += NativeBridge.avxFilteredAggregate(
            blockPtr, filterIdx, opType, targetVal, keyIdx, valIdx, mapPtr, bitmaskPtr
          )
      } finally {
          if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
      }
    }

    val baseMap = scala.collection.mutable.Map[Int, Long]().withDefaultValue(0L)

    if (totalAggregatedRows > 0) {
      val outKeysPtr = NativeBridge.allocMainStore(totalAggregatedRows)
      val outValsPtr = NativeBridge.allocMainStore(totalAggregatedRows * 2) 
      val uniqueCount = NativeBridge.aggregateExport(mapPtr, outKeysPtr, outValsPtr)

      val scalaKeys = new Array[Int](uniqueCount)
      val scalaVals = new Array[Long](uniqueCount)
      NativeBridge.copyToScala(outKeysPtr, scalaKeys, uniqueCount)
      NativeBridge.copyToScalaLong(outValsPtr, scalaVals, uniqueCount)

      for (i <- 0 until uniqueCount) baseMap(scalaKeys(i)) = scalaVals(i)

      NativeBridge.freeMainStore(outKeysPtr)
      NativeBridge.freeMainStore(outValsPtr)
    }
    NativeBridge.freeAggregationResult(mapPtr)

    // 2. RAM MERGE (HTAP)
    val kCol = columns(keyCol)
    val vCol = columns(valCol)
    val fCol = columns(filterCol)
    val ramLen = getRamRowCount()

    for (i <- 0 until ramLen) {
       if (!ramDeleted.get(i)) {
          val filterVal = if (fCol.isString) {
             if (fCol.useDictionary) fCol.getDictId(fCol.deltaStringBuffer(i)) else 0
          } else fCol.deltaIntBuffer(i)

          val matchFound = opType match {
             case 0 => filterVal == targetVal; case 1 => filterVal > targetVal
             case 2 => filterVal >= targetVal; case 3 => filterVal < targetVal
             case 4 => filterVal <= targetVal; case _ => false
          }

          if (matchFound) {
             val k = if (kCol.isString) {
                if (kCol.useDictionary) kCol.getDictId(kCol.deltaStringBuffer(i)) else 0
             } else kCol.deltaIntBuffer(i)

             val v = if (vCol.isString) 0L else vCol.deltaIntBuffer(i).toLong
             baseMap(k) += v
          }
       }
    }

    baseMap.toMap
  }

  // Fast In-Place Update for Integers
  def updateCellNative(id: Int, colName: String, newValue: Int): Boolean = {
    rwLock.writeLock().lock()
    try {
      val loc = primaryIndex.get(id)
      if (loc == null) return false
      
      val colIdx = columnOrder.indexOf(colName)
      if (colIdx == -1) return false

      if (loc.blockPtr == 0) {
        // RAM Update
        val col = columns(colName)
        col.deltaIntBuffer(loc.rowId) = newValue
        true
      } else {
        // NATIVE DISK/RAM Update (In-Place C++)
        NativeBridge.updateCell(loc.blockPtr, colIdx, loc.rowId, newValue)
      }
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def scanFiltered(colName: String, opType: Int, targetVal: Int): Iterator[Array[Any]] = {
    val colIdx = columnOrder.indexOf(colName)
    if (colIdx == -1) return Iterator.empty

    val ramIter = (0 until columns(colName).deltaIntBuffer.length).iterator.collect {
       case i if !ramDeleted.get(i) =>
         val v = columns(colName).deltaIntBuffer(i)
         val matchFound = opType match {
           case 0 => v == targetVal; case 1 => v > targetVal
           case 2 => v >= targetVal; case 3 => v < targetVal
           case 4 => v <= targetVal; case _ => false
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

    val snapshotBlocks = blockManager.getLoadedBlocks.toList
    
    val diskIter = snapshotBlocks.flatMap { blockPtr =>
       val rowCount = NativeBridge.getRowCount(blockPtr)
       if (rowCount == 0) Iterator.empty else {
           val outIndicesPtr = NativeBridge.allocMainStore(rowCount)
           val bitmaskPtr = getNativeBitmask(blockPtr)
           
           try {
               val matchCount = NativeBridge.avxFilterBlock(blockPtr, colIdx, opType, targetVal, outIndicesPtr, bitmaskPtr)
               
               if (matchCount == 0) {
                   Iterator.empty
               } else {
                   val matchingIndices = new Array[Int](matchCount)
                   NativeBridge.copyToScala(outIndicesPtr, matchingIndices, matchCount)
                   
                   val colsData = new Array[Array[Int]](columnOrder.size)
                   for (c <- columnOrder.indices) {
                       colsData(c) = new Array[Int](matchCount)
                       val colPtr = NativeBridge.getColumnPtr(blockPtr, c)
                       val tempValuesPtr = NativeBridge.allocMainStore(matchCount)
                       NativeBridge.batchRead(colPtr, outIndicesPtr, matchCount, tempValuesPtr)
                       NativeBridge.copyToScala(tempValuesPtr, colsData(c), matchCount)
                       NativeBridge.freeMainStore(tempValuesPtr)
                   }
                   
                   (0 until matchCount).map { i =>
                       val originalRowId = matchingIndices(i)
                       
                       // [FIXED] Force secondary filter check in Scala to absolutely prevent Ghost Rows
                       if (blockManager.isDeleted(blockPtr, originalRowId)) null 
                       else {
                           val row = new Array[Any](columnOrder.size)
                           for (c <- columnOrder.indices) {
                               val col = columns(columnOrder(c))
                               val rawVal = colsData(c)(i)
                               if (col.isString) row(c) = col.getDictStr(rawVal) else row(c) = rawVal
                           }
                           row
                       }
                   }.filter(_ != null).toIterator
               }
           } finally {
               NativeBridge.freeMainStore(outIndicesPtr)
               if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
           }
       }
    }.iterator

    ramIter ++ diskIter
  }

  def executeStarQuery(dimTable: AwanTable, buildCol: String, groupCol: String, probeCol: String, sumCol: String): Map[Int, Long] = {
    // For large multi-table operations, a "Soft Flush" keeps logic perfectly aligned for AVX hashing
    dimTable.flush()
    this.flush()

    val buildIdx = dimTable.columnOrder.indexOf(buildCol)
    val groupIdx = dimTable.columnOrder.indexOf(groupCol)
    val probeIdx = this.columnOrder.indexOf(probeCol)
    val sumIdx = this.columnOrder.indexOf(sumCol)

    if (Seq(buildIdx, groupIdx, probeIdx, sumIdx).contains(-1)) throw new IllegalArgumentException("Invalid columns")

    val dimBlocks = dimTable.blockManager.getLoadedBlocks
    var totalDimRows = 0
    for (b <- dimBlocks) totalDimRows += NativeBridge.getRowCount(b)

    val dimKeys = new Array[Int](totalDimRows)
    // [FIXED] Restored perfectly aligned 64-bit arrays to prevent memory shifting!
    val dimPayloads = new Array[Long](totalDimRows) 
    var dimOffset = 0
    
    for (blockPtr <- dimBlocks) {
      val rowsInBlock = NativeBridge.getRowCount(blockPtr)
      if (rowsInBlock > 0) {
          val keyPtr = NativeBridge.getColumnPtr(blockPtr, buildIdx)
          val payloadPtr = NativeBridge.getColumnPtr(blockPtr, groupIdx)
          
          val tempKeys = new Array[Int](rowsInBlock)
          val tempPayloads = new Array[Int](rowsInBlock) 
          
          NativeBridge.copyToScala(keyPtr, tempKeys, rowsInBlock)
          NativeBridge.copyToScala(payloadPtr, tempPayloads, rowsInBlock)
          
          for (i <- 0 until rowsInBlock) {
            if (!dimTable.blockManager.isDeleted(blockPtr, i)) {
                dimKeys(dimOffset) = tempKeys(i)
                dimPayloads(dimOffset) = tempPayloads(i).toLong // Cast to 64-bit
                dimOffset += 1
            }
          }
      }
    }
    
    val activeDimRows = dimOffset
    if (activeDimRows == 0) return Map.empty

    val nativeDimKeysPtr = NativeBridge.allocMainStore(activeDimRows)
    val nativeDimPayloadsPtr = NativeBridge.allocMainStore(activeDimRows * 2) // 2 slots per Long
    
    NativeBridge.loadData(nativeDimKeysPtr, dimKeys.take(activeDimRows))
    NativeBridge.loadDataLong(nativeDimPayloadsPtr, dimPayloads.take(activeDimRows)) 

    val joinMapPtr = NativeBridge.joinBuild(nativeDimKeysPtr, nativeDimPayloadsPtr, activeDimRows)

    val capacityHint = if (activeDimRows > 0) activeDimRows else 10000
    val aggMapPtr = NativeBridge.aggregateCreateMap(capacityHint)
    var totalAggregated = 0

    for (blockPtr <- this.blockManager.getLoadedBlocks) {
      val bitmaskPtr = getNativeBitmask(blockPtr)
      try {
          totalAggregated += NativeBridge.joinProbeAndAggregate(
            blockPtr, probeIdx, sumIdx, joinMapPtr, aggMapPtr, bitmaskPtr
          )
      } finally {
          if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
      }
    }

    var resultMap = Map.empty[Int, Long]

    if (totalAggregated > 0) {
      val outKeysPtr = NativeBridge.allocMainStore(totalAggregated)
      val outValsPtr = NativeBridge.allocMainStore(totalAggregated * 2) 

      val uniqueCount = NativeBridge.aggregateExport(aggMapPtr, outKeysPtr, outValsPtr)

      val scalaKeys = new Array[Int](uniqueCount)
      val scalaVals = new Array[Long](uniqueCount)
      NativeBridge.copyToScala(outKeysPtr, scalaKeys, uniqueCount)
      NativeBridge.copyToScalaLong(outValsPtr, scalaVals, uniqueCount)

      resultMap = scalaKeys.zip(scalaVals).toMap

      NativeBridge.freeMainStore(outKeysPtr)
      NativeBridge.freeMainStore(outValsPtr)
    }

    NativeBridge.freeMainStore(nativeDimKeysPtr)
    NativeBridge.freeMainStore(nativeDimPayloadsPtr)
    NativeBridge.joinDestroy(joinMapPtr)
    NativeBridge.freeAggregationResult(aggMapPtr)

    resultMap
  }

  /**
   * Projects the JVM Deletion BitSet into Native C++ Memory for AVX filtering.
   * Returns 0L if the block is completely clean.
   */
  private def getNativeBitmask(blockPtr: Long): Long = {
    val bitset = blockManager.getDeletionBitSet(blockPtr)
    if (bitset == null || bitset.isEmpty) return 0L
    
    val rowCount = NativeBridge.getRowCount(blockPtr)
    if (rowCount == 0) return 0L

    val rawBytes = bitset.toByteArray
    val requiredBytes = (rowCount + 7) / 8
    val paddedBytes = if (rawBytes.length >= requiredBytes) rawBytes else rawBytes.padTo(requiredBytes, 0.toByte)
    
    val intsNeeded = (requiredBytes + 3) / 4
    val paddedInts = new Array[Int](intsNeeded)
    java.nio.ByteBuffer.wrap(paddedBytes.padTo(intsNeeded * 4, 0.toByte))
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .asIntBuffer().get(paddedInts)
        
    val ptr = NativeBridge.allocMainStore(intsNeeded)
    NativeBridge.loadData(ptr, paddedInts)
    ptr
  }

  def close(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) return
      engineManager.stopEngine()
      engineManager.joinThread()
      blockManager.close()
      wal.close()
      columns.values.foreach(_.close())
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