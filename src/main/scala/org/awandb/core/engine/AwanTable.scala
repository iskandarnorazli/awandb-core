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

// Location Pointer for Primary Index
case class RowLocation(blockPtr: Long, rowId: Int)

class AwanTable(
    val name: String, 
    val capacity: Int, 
    val dataDir: String = "data",
    val governor: EngineGovernor = NoOpGovernor,
    val enableIndex: Boolean = true 
) {
  
  // COMPONENTS
  val wal = new Wal(dataDir)
  val blockManager = new BlockManager(dataDir, enableIndex)
  val columns = new LinkedHashMap[String, NativeColumn]()
  private val columnOrder = new ListBuffer[String]()
  
  // [PERFORMANCE] Pre-allocated Buffer
  val resultIndexBuffer: Long = NativeBridge.allocMainStore(capacity)
  
  private val rwLock = new ReentrantReadWriteLock()
  
  @volatile private var isClosed = false

  // [INDEX] Primary Key Index (Maps ID -> Location)
  private val primaryIndex = new ConcurrentHashMap[Int, RowLocation]()

  // [DELETION] RAM Deletion Bitmap (For rows in Delta Buffer)
  private val ramDeleted = new java.util.BitSet()
  
  val engineManager = new EngineManager(this, governor) 
  engineManager.start()

  blockManager.recover()

  // ---------------------------------------------------------
  // SCHEMA
  // ---------------------------------------------------------
  
  def addColumn(colName: String, isString: Boolean = false, useDictionary: Boolean = false): Unit = {
    rwLock.writeLock().lock()
    try {
      if (isClosed) throw new IllegalStateException("Table is closed")
      if (!columns.contains(colName)) {
        columns += (colName -> new NativeColumn(colName, isString, useDictionary))
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
  // CRUD OPERATIONS
  // ---------------------------------------------------------

  def delete(id: Int): Boolean = {
    rwLock.writeLock().lock()
    try {
      val loc = primaryIndex.get(id)
      if (loc == null) return false

      if (loc.blockPtr == 0) {
        ramDeleted.set(loc.rowId)
      } else {
        blockManager.markDeleted(loc.blockPtr, loc.rowId)
      }
      
      primaryIndex.remove(id)
      true
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  def update(id: Int, changes: Map[String, Any]): Boolean = {
    rwLock.writeLock().lock()
    try {
      if (!primaryIndex.containsKey(id)) return false
      delete(id)
      true 
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
            else throw new IllegalArgumentException(s"Column '$colName' expects Int, but got String.")
          case _ => throw new UnsupportedOperationException(s"Type not supported: ${value.getClass}")
        }
      }

      primaryIndex.put(id, RowLocation(0, currentRamRowId))

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
            if (col.isString) {
              if (col.useDictionary) col.encodeDelta() else col.toStringArray
            } else {
              col.toIntArray
            }
          }.toList
          
          // 2. Persist Data Block
          blockManager.createAndPersistBlock(allColumnsData.asInstanceOf[List[Any]])
    
          // 3. Update Index Locations (RAM -> Disk)
          // [SAFETY] Check if block was actually created
          if (blockManager.getLoadedBlocks.nonEmpty) {
              val newBlockPtr = blockManager.getLoadedBlocks.last
              val rowCount = headCol.get.deltaIntBuffer.length
              val firstCol = columns.values.head
              
              // [FIX] Only migrate index if the ID column is Int-based.
              // Accessing deltaIntBuffer on a String column is invalid/empty.
              if (!firstCol.isString) {
                  val idColData = firstCol.deltaIntBuffer
                  for (i <- 0 until rowCount) {
                    if (!ramDeleted.get(i)) {
                       val id = idColData(i)
                       primaryIndex.put(id, RowLocation(newBlockPtr, i))
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
       if (blockManager.isClean(blockPtr)) {
           NativeBridge.avxScanBlock(blockPtr, colIdx, threshold, 0)
       } else {
           val rowCount = NativeBridge.getRowCount(blockPtr)
           val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
           val data = new Array[Int](rowCount)
           NativeBridge.copyToScala(colPtr, data, rowCount)
           
           var localCount = 0
           var i = 0
           while (i < rowCount) {
             if (!blockManager.isDeleted(blockPtr, i)) {
                if (data(i) > threshold) localCount += 1
             }
             i += 1
           }
           localCount
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

    val allBlocks = blockManager.getLoadedBlocks.toSeq
    if (allBlocks.isEmpty) return Map.empty

    val cores = MorselExec.activeCores
    val blockSize = math.ceil(allBlocks.size.toDouble / cores).toInt
    val blockChunks = allBlocks.grouped(blockSize).toSeq

    val tasks = blockChunks.map { subset =>
      new Callable[scala.collection.mutable.Map[Int, Long]] {
        override def call(): scala.collection.mutable.Map[Int, Long] = {
          val scanOp = new TableScanOperator(blockManager, subset.toArray, keyIdx, valIdx)
          val aggOp = new HashAggOperator(scanOp)
          aggOp.open()
          val resultBatch = aggOp.next() 
          val localMap = scala.collection.mutable.Map[Int, Long]()
          if (resultBatch != null && resultBatch.count > 0) {
             val keys = new Array[Int](resultBatch.count)
             val vals = new Array[Long](resultBatch.count)
             NativeBridge.copyToScala(resultBatch.keysPtr, keys, resultBatch.count)
             NativeBridge.copyToScalaLong(resultBatch.valuesPtr, vals, resultBatch.count)
             var i = 0
             while (i < resultBatch.count) {
               localMap(keys(i)) = vals(i)
               i += 1
             }
          }
          aggOp.close()
          localMap
        }
      }
    }
    val partialResults = MorselExec.runParallel(tasks)
    val finalMap = scala.collection.mutable.Map[Int, Long]()
    for (partial <- partialResults) {
      for ((k, v) <- partial) {
        val current = finalMap.getOrElse(k, 0L)
        finalMap(k) = current + v
      }
    }
    finalMap.toMap
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