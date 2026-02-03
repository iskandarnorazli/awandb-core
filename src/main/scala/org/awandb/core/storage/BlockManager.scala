/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.storage

import java.io.File
import scala.collection.mutable.ArrayBuffer
import org.awandb.core.jni.NativeBridge

// ==================================================================================
// [OPEN CORE ARCHITECTURE] STORAGE ROUTER INTERFACE
// This allows the Enterprise Edition to inject Disk Sharding logic.
// ==================================================================================

trait StorageRouter {
  /**
   * Determines the physical file path for a new block.
   * OSS Implementation: Always returns "$dataDir/block_$id.udb"
   * Enterprise Implementation: Returns "/mnt/nvme_N/block_$id.udb" based on NUMA node.
   */
  def getPathForBlock(blockId: Int): String
  
  /**
   * Called to initialize storage (mkdir, check permissions).
   */
  def initialize(): Unit
}

/**
 * [OSS DEFAULT] Simple Router.
 * Stores everything in a single directory.
 */
class SimpleStorageRouter(dataDir: String) extends StorageRouter {
  override def initialize(): Unit = new File(dataDir).mkdirs()
  
  override def getPathForBlock(blockId: Int): String = {
    f"$dataDir/block_$blockId%05d.udb"
  }
}

// ==================================================================================
// BLOCK MANAGER
// Now delegates file placement to the Router and handles MULTI-COLUMN blocks.
// ==================================================================================

class BlockManager(router: StorageRouter) {
  
  // Auxiliary Constructor for OSS simplicity (defaults to SimpleStorageRouter)
  def this(dataDir: String) = this(new SimpleStorageRouter(dataDir))

  // Simple ID counter for blocks (in real DB, this persists in a manifest file)
  private var blockCounter = 0
  
  // Keep track of loaded blocks (In-Memory Buffer Pool)
  private val loadedBlocks = new ArrayBuffer[Long]() 

  // Initialize storage (create directories)
  router.initialize()

  /**
   * Persist MULTIPLE columns into a single Block.
   * This is the Multi-Column version of the persistence logic.
   *
   * @param columnsData Sequence of arrays, one for each column.
   */
  def createAndPersistBlock(columnsData: Seq[Array[Int]]): Unit = {
    if (columnsData.isEmpty) return

    val rowCount = columnsData.head.length
    val colCount = columnsData.length
    
    // 1. Create Block in Unified Memory (C++) with N columns
    // Allocates the Block Header + Column Headers + Data Space
    val blockPtr = NativeBridge.createBlock(rowCount, colCount)
    
    // 2. Copy Data for EACH column
    for (colIdx <- 0 until colCount) {
      val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
      NativeBridge.loadData(colPtr, columnsData(colIdx))
    }
    
    // 3. Save to Disk
    // [ENT HOOK] The Router decides physical placement (Local Disk vs NVMe Shard)
    val filename = router.getPathForBlock(blockCounter)
    
    val blockSize = NativeBridge.getBlockSize(blockPtr)
    
    val saved = NativeBridge.saveColumn(blockPtr, blockSize, filename) 
    if (!saved) {
      NativeBridge.freeMainStore(blockPtr)
      throw new RuntimeException(s"Failed to save block to disk at $filename")
    }

    println(f"[BlockManager] Persisted Block $blockCounter ($rowCount rows, $colCount cols) to $filename")

    // 4. Register Block
    // Ideally, we free the pointer here if we are out of RAM (Buffer Manager logic).
    // For now, we keep it loaded for fast querying.
    loadedBlocks.append(blockPtr)
    blockCounter += 1
  }

  def getLoadedBlocks: Seq[Long] = loadedBlocks.toSeq

  def close(): Unit = {
    loadedBlocks.foreach(NativeBridge.freeMainStore)
    loadedBlocks.clear()
  }
}