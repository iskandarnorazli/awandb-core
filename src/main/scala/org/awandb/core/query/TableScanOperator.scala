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

class TableScanOperator(
    val blocks: Array[Long], 
    val keyColIdx: Int, 
    val valColIdx: Int,
    val batchSize: Int = 4096
) extends Operator {
  
  def this(bm: BlockManager, k: Int, v: Int) = this(bm.getLoadedBlocks.toArray, k, v)

  private var currentBlockIdx = 0
  private var currentRowInBlock = 0
  private var batch: VectorBatch = _

  override def open(): Unit = {
    currentBlockIdx = 0
    currentRowInBlock = 0
    batch = new VectorBatch(batchSize)
  }

  private def readColumnChunk(
      blockPtr: Long, 
      colIdx: Int, 
      startRow: Int, 
      count: Int, 
      dstPtr: Long, 
      dstOffsetBytes: Long
  ): Unit = {
    var stride = NativeBridge.getColumnStride(blockPtr, colIdx)
    
    // [SAFETY] Safety net for uninitialized blocks (stride=0)
    // If stride is 0, we assume standard 4-byte Integers.
    if (stride == 0) {
       stride = 4
    }
    
    val colBase = NativeBridge.getColumnPtr(blockPtr, colIdx)
    val srcOffset = startRow.toLong * stride.toLong
    val srcPtr = NativeBridge.getOffsetPointer(colBase, srcOffset)
    val finalDstPtr = NativeBridge.getOffsetPointer(dstPtr, dstOffsetBytes)
    
    stride match {
      case 4 => NativeBridge.memcpy(srcPtr, finalDstPtr, count * 4L)
      case 1 => NativeBridge.unpack8To32(srcPtr, finalDstPtr, count)
      case 2 => NativeBridge.unpack16To32(srcPtr, finalDstPtr, count)
      case _ => NativeBridge.memcpy(srcPtr, finalDstPtr, count * 4L)
    }
  }

  override def next(): VectorBatch = {
    if (currentBlockIdx >= blocks.length) return null

    var rowsFilled = 0
    // [LATE MAT] Track which block this batch belongs to. 
    var activeBlockPtr: Long = 0
    // [CRITICAL FIX] Capture the starting row for this batch relative to the block.
    // This allows MaterializeOperator to calculate the correct memory offset later.
    var batchStartRow = -1 

    while (rowsFilled < batchSize && currentBlockIdx < blocks.length) {
      val blockPtr = blocks(currentBlockIdx)
      
      if (blockPtr == 0) {
        currentBlockIdx += 1
        currentRowInBlock = 0
      } else {
        if (activeBlockPtr == 0) {
            activeBlockPtr = blockPtr
            // [FIX] Set the offset for the entire batch based on the first row we touch
            batchStartRow = currentRowInBlock
        }
        
        // [LATE MAT CONSTRAINT] If we cross into a NEW block, we must stop this batch here.
        if (blockPtr != activeBlockPtr) {
            batch.count = rowsFilled
            batch.blockPtr = activeBlockPtr
            batch.startRowInBlock = batchStartRow // Pass offset
            return batch
        }

        val totalRows = NativeBridge.getRowCount(blockPtr)
        val remainingInBlock = totalRows - currentRowInBlock
        val spaceInBatch = batchSize - rowsFilled
        val toCopy = math.min(remainingInBlock, spaceInBatch)
        
        if (toCopy > 0) {
          val destOffset = rowsFilled * 4L
          readColumnChunk(blockPtr, keyColIdx, currentRowInBlock, toCopy, batch.keysPtr, destOffset)
          readColumnChunk(blockPtr, valColIdx, currentRowInBlock, toCopy, batch.valuesPtr, destOffset)
          
          rowsFilled += toCopy
          currentRowInBlock += toCopy
        }
        
        if (currentRowInBlock >= totalRows) {
          currentBlockIdx += 1
          currentRowInBlock = 0
        }
      }
    }
    
    if (rowsFilled == 0) return null
    
    batch.count = rowsFilled
    batch.blockPtr = activeBlockPtr
    batch.startRowInBlock = batchStartRow // Pass offset
    
    batch
  }

  override def close(): Unit = {
    if (batch != null) batch.close()
  }
}