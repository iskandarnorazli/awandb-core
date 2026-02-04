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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.storage

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CopyOnWriteArrayList}
import scala.collection.JavaConverters._
import org.awandb.core.jni.NativeBridge

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

  // Atomic counter for thread safety
  private val blockCounter = new java.util.concurrent.atomic.AtomicInteger(0)

  // [THREAD SAFETY] CopyOnWriteArrayList for lock-free reads
  private val loadedBlocks = new CopyOnWriteArrayList[Long]() 
  
  // [THREAD SAFETY] Concurrent map for O(1) lookups
  private val loadedFilters = new ConcurrentHashMap[Int, Long]() 
  
  // [THREAD SAFETY] Queue for the background worker
  // We use java.lang.Integer to safely handle nulls when polling empty queue
  private val pendingIndexes = new ConcurrentLinkedQueue[java.lang.Integer]()

  router.initialize()

  /**
   * RECOVERY: Loads blocks. If index file exists, load it. If not, queue it.
   */
  def recover(): Unit = {
    val dir = new File(router.getDataDir)
    if (!dir.exists() || !dir.isDirectory) return

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
                  // Index missing -> Queue for background build
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
   * Does NOT build the Cuckoo filter immediately.
   * Returns fast, queues work for later.
   * * [DENSE ENGINE] Accepts raw Int arrays (Reverted from NativeColumn for stability)
   */
  def createAndPersistBlock(columnsData: Seq[Array[Int]]): Unit = {
    if (columnsData.isEmpty) return

    val rowCount = columnsData.head.length
    val colCount = columnsData.length
    val currentId = blockCounter.getAndIncrement()
    
    // 1. Create Data Block (Dense Integer Layout)
    val blockPtr = NativeBridge.createBlock(rowCount, colCount)
    for (colIdx <- 0 until colCount) {
      val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
      NativeBridge.loadData(colPtr, columnsData(colIdx))
    }
    
    // 2. Save Data to Disk
    val filename = router.getPathForBlock(currentId)
    val saved = NativeBridge.saveColumn(blockPtr, NativeBridge.getBlockSize(blockPtr), filename) 
    
    if (!saved) {
      NativeBridge.freeMainStore(blockPtr)
      throw new RuntimeException(s"Failed to save block: $filename")
    }

    // 3. Register Block
    loadedBlocks.add(blockPtr)
    
    // 4. Queue for Background Indexing
    if (enableIndex && rowCount > 0) {
        // Queue the logic index (which matches our list index)
        pendingIndexes.offer(loadedBlocks.size() - 1)
    }
  }

  /**
   * [BACKGROUND WORKER] Builds pending indexes.
   * Returns 1 if work was done, 0 if queue was empty.
   */
  def buildPendingIndexes(): Int = {
    val blockIdx = pendingIndexes.poll()
    
    // Safe check for null (Queue Empty)
    if (blockIdx == null) return 0

    // Safety checks
    if (blockIdx >= loadedBlocks.size()) return 0 
    if (loadedFilters.containsKey(blockIdx)) return 0 

    val blockPtr = loadedBlocks.get(blockIdx)
    val rowCount = NativeBridge.getRowCount(blockPtr)
    if (rowCount == 0) return 0

    // 1. Fetch Data (Assume Primary Key is Col 0)
    val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
    val data = new Array[Int](rowCount)
    NativeBridge.copyToScala(colPtr, data, rowCount)

    // 2. Build Filter (The slow part)
    val filterPtr = NativeBridge.cuckooCreate((rowCount * 1.5).toInt)
    NativeBridge.cuckooBuildBatch(filterPtr, data)
    
    // 3. Save
    // NOTE: This assumes blockId == listIndex (Valid for append-only)
    val path = router.getPathForFilter(blockIdx) 
    NativeBridge.cuckooSave(filterPtr, path)
    
    // 4. Publish (Atomic Put makes it visible to readers)
    loadedFilters.put(blockIdx, filterPtr)
    
    // println(s"[Indexer] Built background index for block $blockIdx")
    1 
  }

  // [O(1) POINT LOOKUP]
  def mightContain(blockIndex: Int, key: Int): Boolean = {
    if (!enableIndex) return true
    
    val ptr = loadedFilters.get(blockIndex)
    // If index not ready (null/0), return true to force scan (Safety)
    if (ptr == 0L) return true 
    
    NativeBridge.cuckooContains(ptr, key)
  }

  // [FIX] Explicit conversion to Immutable Seq to match return type
  def getLoadedBlocks: Seq[Long] = loadedBlocks.asScala.toSeq

  def close(): Unit = {
    loadedBlocks.asScala.foreach(NativeBridge.freeMainStore)
    loadedBlocks.clear()
    loadedFilters.values().asScala.foreach(NativeBridge.cuckooDestroy)
    loadedFilters.clear()
  }
}