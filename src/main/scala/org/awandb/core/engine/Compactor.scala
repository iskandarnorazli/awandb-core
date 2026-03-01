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

import org.awandb.core.engine.memory.EpochManager
import org.awandb.core.jni.NativeBridge
import scala.collection.mutable.ArrayBuffer

class Compactor(table: AwanTable, epochManager: EpochManager) {

  /**
   * Scans loaded blocks and merges those exceeding the deletion threshold.
   * @param threshold Decimal percentage (e.g., 0.3 for 30%)
   * @return Number of blocks compacted.
   */
  def compact(threshold: Double): Int = {
    val blockManager = table.blockManager
    val blocksToCompact = new ArrayBuffer[Long]()
    val bitmasksToCompact = new ArrayBuffer[Long]()
    
    // 1. IDENTIFY Phase (Read Lock)
    val blocks = blockManager.getLoadedBlocks
    for (blockPtr <- blocks) {
      val rowCount = NativeBridge.getRowCount(blockPtr)
      val bitset = blockManager.getDeletionBitSet(blockPtr)
      
      if (bitset != null && !bitset.isEmpty) {
        val deletedCount = bitset.cardinality()
        val ratio = deletedCount.toDouble / rowCount.toDouble
        
        if (ratio >= threshold) {
          blocksToCompact.append(blockPtr)
          // Fetch the cached native pointer so C++ can read the deletions
          val nativeBitmask = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
          bitmasksToCompact.append(nativeBitmask)
        }
      }
    }

    if (blocksToCompact.isEmpty) return 0

    // 2. NATIVE MERGE Phase (Lock-Freeish - Done outside the write lock!)
    // We pass the old blocks to C++, which creates a brand new tightly-packed block.
    val newBlockPtr = NativeBridge.compactBlocks(
      blocksToCompact.toArray, 
      bitmasksToCompact.toArray
    )

    // REMOVE THIS LINE:
    // if (newBlockPtr == 0L) throw new RuntimeException("Native compaction failed")

    // 3. POINTER SWAP & RETIRE Phase (Write Lock)
    table.swapCompactedBlocks(blocksToCompact.toArray, newBlockPtr, epochManager)

    blocksToCompact.size
  }
}