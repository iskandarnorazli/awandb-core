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

package org.awandb.core.storage

import java.io.{File, FileOutputStream, ObjectOutputStream, FileInputStream, ObjectInputStream}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CopyOnWriteArrayList}
import scala.jdk.CollectionConverters._ 
import org.awandb.core.jni.NativeBridge
import org.awandb.core.util.UnsafeHelper

trait StorageRouter {
  def getPathForBlock(blockId: Int): String
  def getPathForFilter(blockId: Int): String
  def getPathForBitmap(blockId: Int): String 
  def getDataDir: String
  def initialize(): Unit
}

class SimpleStorageRouter(val dataDir: String, val prefix: String = "") extends StorageRouter {
  // If a prefix (table name) is provided, append an underscore. Otherwise, leave blank.
  private val p = if (prefix.nonEmpty) s"${prefix}_" else ""
  
  override def initialize(): Unit = new java.io.File(dataDir).mkdirs()
  override def getDataDir: String = dataDir
  
  // [FIXED] Prefixes files with Table Name to prevent cross-table memory hijacking!
  override def getPathForBlock(blockId: Int): String = f"$dataDir/${p}block_$blockId%05d.udb"
  override def getPathForFilter(blockId: Int): String = f"$dataDir/${p}block_$blockId%05d.cuckoo"
  override def getPathForBitmap(blockId: Int): String = f"$dataDir/${p}block_$blockId%05d.del" 
}

// ==================================================================================
// THREAD-SAFE BLOCK MANAGER (Lazy Indexing + Deletion Vectors)
// ==================================================================================

class BlockManager(val router: StorageRouter, val enableIndex: Boolean = true) {
  
  // [FIXED] Removed the default `= true` to satisfy the Scala compiler.
  // Overloaded constructors cannot share default arguments with the primary constructor.
  def this(dataDir: String, enableIndex: Boolean) = 
    this(new SimpleStorageRouter(dataDir, ""), enableIndex)

  // Added a single-argument auxiliary constructor for any legacy tests 
  // that still just pass a directory string.
  def this(dataDir: String) = 
    this(new SimpleStorageRouter(dataDir, ""), true)

  private val blockCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  
  private val loadedBlocks = new java.util.concurrent.CopyOnWriteArrayList[Long]() 
  private val loadedFilters = new java.util.concurrent.ConcurrentHashMap[Int, Long]() 
  private val pendingIndexes = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Integer]()
  private val deletionBitmaps = new java.util.concurrent.ConcurrentHashMap[Long, java.util.BitSet]()

  router.initialize()

  /**
   * RECOVERY: Loads blocks, filters, and [NEW] deletion bitmaps.
   */
    def recover(): Unit = {
    var i = 0
    var recovering = true
    while (recovering) {
      val path = router.getPathForBlock(i) 
      val file = new java.io.File(path)
      if (file.exists()) {
         // [FIXED] Actually load the Native C++ Blocks and Bitmaps!
         val ptr = NativeBridge.loadBlockFromFile(path)
         if (ptr != 0L) {
             loadedBlocks.add(ptr)
             loadBitmap(i, ptr)
         }
         i += 1
      } else {
         recovering = false
      }
    }
    println(s"[BlockManager] Recovery Complete. Next Block ID: $i")
  }

  /**
   * [FAST PATH] Create and Persist Block Data ONLY.
   */
  def createAndPersistBlock(columnsData: Seq[Any]): Unit = {
    if (columnsData.isEmpty) return

    val (rowCount, hasString) = columnsData.head match {
      case a: Array[Int] => (a.length, columnsData.exists(_.isInstanceOf[Array[String]]))
      case a: Array[String] => (a.length, true)
      case _ => (0, false)
    }

    if (rowCount == 0) return
    val colCount = columnsData.length
    val currentId = blockCounter.getAndIncrement()
    
    // [STRATEGY: Over-Allocate, Then Patch]
    val allocationRows = if (hasString) rowCount * 16 else rowCount
    val blockPtr = NativeBridge.createBlock(allocationRows, colCount)
    
    // [CRITICAL FIX] ALWAYS patch the header row count.
    // This fixes the "0 items found" bug for Integer-only tables.
    UnsafeHelper.putInt(blockPtr, 8L, rowCount)
    
    try {
        val dataArray = columnsData.toArray
        
        for (colIdx <- 0 until colCount) {
           dataArray(colIdx) match {
               case ints: Array[Int] =>
                   val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
                   NativeBridge.loadData(colPtr, ints)
               case strs: Array[String] =>
                   NativeBridge.loadStringData(blockPtr, colIdx, strs)
               case _ => 
                   throw new IllegalArgumentException(s"Unsupported column type at index $colIdx")
           }
        }
        
        val filename = router.getPathForBlock(currentId)
        val saved = NativeBridge.saveColumn(blockPtr, NativeBridge.getBlockSize(blockPtr), filename) 
        
        if (!saved) {
          throw new RuntimeException(s"Failed to save block: $filename")
        }

        loadedBlocks.add(blockPtr)
        
        if (enableIndex && rowCount > 0) {
            pendingIndexes.offer(loadedBlocks.size() - 1)
        }

    } catch {
        case e: Throwable =>
            NativeBridge.freeMainStore(blockPtr)
            throw e
    }
  }

  def buildPendingIndexes(): Int = {
    val blockIdx = pendingIndexes.poll()
    if (blockIdx == null) return 0
    if (blockIdx >= loadedBlocks.size()) return 0 
    if (loadedFilters.containsKey(blockIdx)) return 0 

    val blockPtr = loadedBlocks.get(blockIdx)
    val rowCount = NativeBridge.getRowCount(blockPtr)
    if (rowCount == 0) return 0

    val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
    val data = new Array[Int](rowCount)
    NativeBridge.copyToScala(colPtr, data, rowCount)

    val filterPtr = NativeBridge.cuckooCreate((rowCount * 1.5).toInt)
    NativeBridge.cuckooBuildBatch(filterPtr, data)
    
    val path = router.getPathForFilter(blockIdx) 
    NativeBridge.cuckooSave(filterPtr, path)
    loadedFilters.put(blockIdx, filterPtr)
    1 
  }

  def mightContain(blockIndex: Int, key: Int): Boolean = {
    if (!enableIndex) return true
    val ptr = loadedFilters.get(blockIndex)
    if (ptr == 0L) return true 
    NativeBridge.cuckooContains(ptr, key)
  }

  def getLoadedBlocks: scala.collection.Seq[Long] = loadedBlocks.asScala.toSeq

  def saveBitmaps(): Unit = {
    val it = loadedBlocks.iterator()
    var idx = 0
    while(it.hasNext) {
      val ptr = it.next()
      val bitset = deletionBitmaps.get(ptr)
      
      if (bitset != null && !bitset.isEmpty) {
         val path = router.getPathForBitmap(idx)
         try {
           val fos = new FileOutputStream(path)
           val oos = new ObjectOutputStream(fos)
           oos.writeObject(bitset)
           oos.close()
         } catch {
           case e: Exception => println(s"[BlockManager] Failed to save bitmap $idx: ${e.getMessage}")
         }
      }
      idx += 1
    }
  }

  def addLoadedBlock(blockPtr: Long): Unit = {
    // Assuming you have a collection like `loadedBlocks: ArrayBuffer[Long]`
    loadedBlocks.add(blockPtr)
  }

  private def loadBitmap(blockIdx: Int, blockPtr: Long): Unit = {
    val path = router.getPathForBitmap(blockIdx)
    val file = new File(path)
    if (file.exists()) {
       try {
         val fis = new FileInputStream(file)
         val ois = new ObjectInputStream(fis)
         val bitset = ois.readObject().asInstanceOf[java.util.BitSet]
         deletionBitmaps.put(blockPtr, bitset)
         ois.close()
         println(s"[BlockManager] Loaded Deletions for Block $blockIdx")
       } catch {
         case e: Exception => println(s"[BlockManager] Failed to load bitmap: ${e.getMessage}")
       }
    }
  }

  def getDeletionBitSet(blockPtr: Long): java.util.BitSet = {
    deletionBitmaps.get(blockPtr)
  }

  def markDeleted(blockPtr: Long, rowId: Int): Unit = {
    deletionBitmaps.computeIfAbsent(blockPtr, _ => new java.util.BitSet()).set(rowId)
  }

  def isClean(blockPtr: Long): Boolean = {
    val bitset = deletionBitmaps.get(blockPtr)
    bitset == null || bitset.isEmpty
  }

  def isDeleted(blockPtr: Long, rowId: Int): Boolean = {
    val bits = deletionBitmaps.get(blockPtr)
    if (bits == null) false else bits.get(rowId)
  }

  def close(): Unit = {
    loadedBlocks.asScala.foreach(NativeBridge.freeMainStore)
    loadedBlocks.clear()
    loadedFilters.values().asScala.foreach(NativeBridge.cuckooDestroy)
    loadedFilters.clear()
    deletionBitmaps.clear()
  }
}