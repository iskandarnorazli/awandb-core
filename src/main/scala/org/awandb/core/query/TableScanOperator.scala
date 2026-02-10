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
    // 1. Reset Batch
    batch.count = 0
    var rowsFilled = 0
    
    // We track the active block to enforce the Late Materialization constraint
    // (A batch generally shouldn't span multiple blocks if downstream needs blockPtr)
    var activeBlockPtr: Long = 0

    // 2. Loop until batch is full or we run out of blocks
    while (rowsFilled < batchSize && currentBlockIdx < blocks.length) {
      val blockPtr = blocks(currentBlockIdx)
      
      // Safety check: Skip null blocks
      if (blockPtr == 0) {
          currentBlockIdx += 1
          currentRowInBlock = 0
      } else {
        
        // Initialize active block for this batch
        if (activeBlockPtr == 0) {
            activeBlockPtr = blockPtr
            batch.blockPtr = activeBlockPtr
            // Note: With deletion filtering, startRowInBlock is less useful 
            // as the batch is no longer contiguous, but we set it for reference.
            batch.startRowInBlock = currentRowInBlock 
        }

        // [LATE MAT CONSTRAINT] If we hit a new block, stop and return what we have.
        if (blockPtr != activeBlockPtr) {
            batch.count = rowsFilled
            return batch
        }

        val totalRows = NativeBridge.getRowCount(blockPtr)
        
        // Resolve Column Pointers
        val keyColBase = NativeBridge.getColumnPtr(blockPtr, keyColIdx)
        val valColBase = NativeBridge.getColumnPtr(blockPtr, valColIdx)
        
        // Resolve Strides (Handle Compression)
        var keyStride = NativeBridge.getColumnStride(blockPtr, keyColIdx)
        if (keyStride == 0) keyStride = 4
        
        var valStride = NativeBridge.getColumnStride(blockPtr, valColIdx)
        if (valStride == 0) valStride = 4

        // [CRITICAL LOOP] Row-by-Row Scan with Deletion Check
        while (currentRowInBlock < totalRows && rowsFilled < batchSize) {
           
           // 1. Check Tombstone (Bitmap)
           if (!blockManager.isDeleted(blockPtr, currentRowInBlock)) {
               
               // 2. Row is ALIVE. Copy Key.
               val srcKey = NativeBridge.getOffsetPointer(keyColBase, currentRowInBlock * keyStride.toLong)
               val dstKey = NativeBridge.getOffsetPointer(batch.keysPtr, rowsFilled * 4L)
               
               keyStride match {
                 case 4 => NativeBridge.memcpy(srcKey, dstKey, 4)
                 case 1 => NativeBridge.unpack8To32(srcKey, dstKey, 1)
                 case 2 => NativeBridge.unpack16To32(srcKey, dstKey, 1)
                 case _ => NativeBridge.memcpy(srcKey, dstKey, 4)
               }

               // 3. Copy Value.
               val srcVal = NativeBridge.getOffsetPointer(valColBase, currentRowInBlock * valStride.toLong)
               val dstVal = NativeBridge.getOffsetPointer(batch.valuesPtr, rowsFilled * 4L) // Values are always 4 bytes here (Int)
               
               valStride match {
                 case 4 => NativeBridge.memcpy(srcVal, dstVal, 4)
                 case 1 => NativeBridge.unpack8To32(srcVal, dstVal, 1)
                 case 2 => NativeBridge.unpack16To32(srcVal, dstVal, 1)
                 case _ => NativeBridge.memcpy(srcVal, dstVal, 4)
               }
               
               rowsFilled += 1
           }
           
           // Move to next row in block regardless of whether we copied it or not
           currentRowInBlock += 1
        }
        
        // Check if block is exhausted
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