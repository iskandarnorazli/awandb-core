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

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CopyOnWriteArrayList}
import scala.jdk.CollectionConverters._ 
import org.awandb.core.jni.NativeBridge
import org.awandb.core.util.UnsafeHelper // [NEW] Required for Header Patching

trait StorageRouter {
  def getPathForBlock(blockId: Int): String
  def getPathForFilter(blockId: Int): String
  def getDataDir: String
  def initialize(): Unit
}

class SimpleStorageRouter(val dataDir: String) extends StorageRouter {
  override def initialize(): Unit = new File(dataDir).mkdirs()
  override def getDataDir: String = dataDir
  override def getPathForBlock(blockId: Int): String = f"$dataDir/block_$blockId%05d.udb"
  override def getPathForFilter(blockId: Int): String = f"$dataDir/block_$blockId%05d.cuckoo"
}

// ==================================================================================
// THREAD-SAFE BLOCK MANAGER (Lazy Indexing)
// ==================================================================================

class BlockManager(router: StorageRouter, val enableIndex: Boolean) {
  
  def this(dataDir: String, enableIndex: Boolean = true) = 
    this(new SimpleStorageRouter(dataDir), enableIndex)

  private val blockCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  
  // [THREAD SAFETY] CopyOnWriteArrayList for lock-free reads
  private val loadedBlocks = new CopyOnWriteArrayList[Long]() 
  private val loadedFilters = new ConcurrentHashMap[Int, Long]() 
  private val pendingIndexes = new ConcurrentLinkedQueue[java.lang.Integer]()

  router.initialize()

  /**
   * RECOVERY: Loads blocks. If index file exists, load it. If not, queue it.
   */
  def recover(): Unit = {
    val dir = new File(router.getDataDir)
    if (!dir.exists() || !dir.isDirectory) return

    loadedBlocks.clear()
    loadedFilters.clear()

    val files = dir.listFiles().filter(f => f.isFile && f.getName.endsWith(".udb") && f.getName.startsWith("block_"))
    val sortedFiles = files.sortBy(_.getName)

    println(s"[BlockManager] Recovering ${sortedFiles.length} blocks...")

    var maxId = -1
    for (file <- sortedFiles) {
      val ptr = NativeBridge.loadBlockFromFile(file.getAbsolutePath)
      if (ptr != 0) {
          loadedBlocks.add(ptr)
          val blockIdx = loadedBlocks.size() - 1
          
          val name = file.getName
          val idPart = name.stripPrefix("block_").stripSuffix(".udb")
          val id = scala.util.Try(idPart.toInt).getOrElse(0)
          if (id > maxId) maxId = id
          
          if (enableIndex) {
              val filterPath = router.getPathForFilter(id)
              if (new File(filterPath).exists()) {
                  val filterPtr = NativeBridge.cuckooLoad(filterPath)
                  if (filterPtr != 0) loadedFilters.put(blockIdx, filterPtr)
              } else {
                  pendingIndexes.offer(blockIdx)
              }
          }
      }
    }
    blockCounter.set(maxId + 1)
    println(s"[BlockManager] Recovery Complete. Next Block ID: ${blockCounter.get()}")
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
    // 1. Allocate 16x memory if strings are present to prevent C++ Heap Overflow.
    //    This creates a valid block memory-wise, but metadata says "Rows = 16 * N".
    val allocationRows = if (hasString) rowCount * 16 else rowCount
    val blockPtr = NativeBridge.createBlock(allocationRows, colCount)
    
    // 2. [PATCH] Immediately correct the header.
    //    We overwrite row_count with the TRUE count. This ensures:
    //    a) Scans stop at the correct row (fixing the 8M vs 500k bug).
    //    b) Saves only write the valid data (fixing disk bloat).
    if (hasString) {
        UnsafeHelper.putInt(blockPtr, rowCount)
    }
    
    try {
        val dataArray = columnsData.toArray
        
        for (colIdx <- 0 until colCount) {
           dataArray(colIdx) match {
               case ints: Array[Int] =>
                   val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
                   NativeBridge.loadData(colPtr, ints)
                   
               case strs: Array[String] =>
                   // Safe now because physical memory is massive (16x)
                   NativeBridge.loadStringData(blockPtr, colIdx, strs)
                   
               case _ => 
                   throw new IllegalArgumentException(s"Unsupported column type at index $colIdx")
           }
        }
        
        // 4. Save to Disk
        // saveColumn reads the header we patched, so it writes a compact file.
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

  def close(): Unit = {
    loadedBlocks.asScala.foreach(NativeBridge.freeMainStore)
    loadedBlocks.clear()
    loadedFilters.values().asScala.foreach(NativeBridge.cuckooDestroy)
    loadedFilters.clear()
  }
}