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

package org.awandb.core.query

import org.awandb.core.jni.NativeBridge
import org.awandb.core.storage.BlockManager

// [CHANGE] We require BlockManager to check for deletions
class TableScanOperator(
    val blockManager: BlockManager,
    val blocks: Array[Long], 
    val keyColIdx: Int, 
    val valColIdx: Int,
    val batchSize: Int = 4096
) extends Operator {
  
  // Auxiliary Constructor for tests/legacy calls
  def this(bm: BlockManager, k: Int, v: Int) = 
      this(bm, bm.getLoadedBlocks.toArray, k, v)

  private var currentBlockIdx = 0
  private var currentRowInBlock = 0
  private var batch: VectorBatch = _

  override def open(): Unit = {
    currentBlockIdx = 0
    currentRowInBlock = 0
    batch = new VectorBatch(batchSize)
  }

  override def next(): VectorBatch = {
    batch.count = 0
    var rowsFilled = 0
    
    var currentBatchBlockPtr: Long = 0
    var currentBatchIsClean: Boolean = false // [NEW] Cache the map lookup

    while (rowsFilled < batchSize && currentBlockIdx < blocks.length) {
      val blockPtr = blocks(currentBlockIdx)
      
      if (blockPtr == 0) {
          currentBlockIdx += 1
          currentRowInBlock = 0
      } else {
        
        // [FIX] Initialize batch tracking and do the HashMap lookup exactly ONCE
        if (currentBatchBlockPtr == 0) {
            currentBatchBlockPtr = blockPtr
            batch.blockPtr = currentBatchBlockPtr
            batch.startRowInBlock = currentRowInBlock 
            
            // Query the ConcurrentHashMap only once per 4096-row batch
            currentBatchIsClean = blockManager.isClean(blockPtr) 
        }

        if (blockPtr != currentBatchBlockPtr) {
            batch.count = rowsFilled
            return batch
        }

        val totalRows = NativeBridge.getRowCount(blockPtr)
        val keyColBase = NativeBridge.getColumnPtr(blockPtr, keyColIdx)
        val valColBase = NativeBridge.getColumnPtr(blockPtr, valColIdx)
        
        var keyStride = NativeBridge.getColumnStride(blockPtr, keyColIdx)
        if (keyStride == 0) keyStride = 4
        
        var valStride = NativeBridge.getColumnStride(blockPtr, valColIdx)
        if (valStride == 0) valStride = 4

        // [FIX] Use the cached boolean instead of calling the BlockManager again
        if (currentBatchIsClean && keyStride == 4 && valStride == 4) {
            val rowsToCopy = math.min(batchSize - rowsFilled, totalRows - currentRowInBlock)
            
            val srcKey = NativeBridge.getOffsetPointer(keyColBase, currentRowInBlock * 4L)
            val dstKey = NativeBridge.getOffsetPointer(batch.keysPtr, rowsFilled * 4L)
            NativeBridge.memcpy(srcKey, dstKey, rowsToCopy * 4L)
            
            val srcVal = NativeBridge.getOffsetPointer(valColBase, currentRowInBlock * 4L)
            val dstVal = NativeBridge.getOffsetPointer(batch.valuesPtr, rowsFilled * 4L)
            NativeBridge.memcpy(srcVal, dstVal, rowsToCopy * 4L)
            
            rowsFilled += rowsToCopy
            currentRowInBlock += rowsToCopy
            
        } else {
            while (currentRowInBlock < totalRows && rowsFilled < batchSize) {
               if (!blockManager.isDeleted(blockPtr, currentRowInBlock)) {
                   
                   val srcKey = NativeBridge.getOffsetPointer(keyColBase, currentRowInBlock * keyStride.toLong)
                   val dstKey = NativeBridge.getOffsetPointer(batch.keysPtr, rowsFilled * 4L)
                   
                   keyStride match {
                     case 4 => NativeBridge.memcpy(srcKey, dstKey, 4)
                     case 1 => NativeBridge.unpack8To32(srcKey, dstKey, 1)
                     case 2 => NativeBridge.unpack16To32(srcKey, dstKey, 1)
                     case _ => NativeBridge.memcpy(srcKey, dstKey, 4)
                   }

                   val srcVal = NativeBridge.getOffsetPointer(valColBase, currentRowInBlock * valStride.toLong)
                   val dstVal = NativeBridge.getOffsetPointer(batch.valuesPtr, rowsFilled * 4L)
                   
                   valStride match {
                     case 4 => NativeBridge.memcpy(srcVal, dstVal, 4)
                     case 1 => NativeBridge.unpack8To32(srcVal, dstVal, 1)
                     case 2 => NativeBridge.unpack16To32(srcVal, dstVal, 1)
                     case _ => NativeBridge.memcpy(srcVal, dstVal, 4)
                   }
                   
                   rowsFilled += 1
               }
               currentRowInBlock += 1
            }
        }
        
        if (currentRowInBlock >= totalRows) {
          currentBlockIdx += 1
          currentRowInBlock = 0
        }
      }
    }
    
    if (rowsFilled == 0) return null
    batch.count = rowsFilled
    batch
  }

  override def close(): Unit = {
    if (batch != null) {
        NativeBridge.freeMainStore(batch.keysPtr)
        NativeBridge.freeMainStore(batch.valuesPtr)
    }
  }
}