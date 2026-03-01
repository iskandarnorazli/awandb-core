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

  def compact(threshold: Double): Int = {
    val blockManager = table.blockManager
    val blocksToCompact = new ArrayBuffer[Long]()
    val bitmasksToCompact = new ArrayBuffer[Long]()
    
    // Acquire Read Lock to guarantee memory visibility across APU cores
    table.rwLock.readLock().lock()
    try {
      val blocks = blockManager.getLoadedBlocks
      for (blockPtr <- blocks) {
        val rowCount = NativeBridge.getRowCount(blockPtr)
        val bitset = blockManager.getDeletionBitSet(blockPtr)
        
        if (bitset != null && !bitset.isEmpty) {
          val deletedCount = bitset.cardinality()
          val ratio = deletedCount.toDouble / rowCount.toDouble
          
          if (ratio >= threshold) {
            blocksToCompact.append(blockPtr)
            val nativeBitmask = blockManager.getNativeDeletionBitmap(blockPtr, rowCount)
            bitmasksToCompact.append(nativeBitmask)
          }
        }
      }
    } finally {
      table.rwLock.readLock().unlock()
    }

    if (blocksToCompact.isEmpty) return 0

    // 2. NATIVE MERGE Phase (Done outside the lock!)
    val newBlockPtr = NativeBridge.compactBlocks(
      blocksToCompact.toArray, 
      bitmasksToCompact.toArray
    )

    // 3. POINTER SWAP Phase
    table.swapCompactedBlocks(blocksToCompact.toArray, newBlockPtr, epochManager)
    blocksToCompact.size
  }
}