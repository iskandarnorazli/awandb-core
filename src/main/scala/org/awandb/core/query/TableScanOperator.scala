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

// -------------------------------------------------------------------------
// TABLE SCAN OPERATOR (Hybrid Reader)
// Automatically handles Uncompressed (4B), Packed-8 (1B), and Packed-16 (2B) data.
// -------------------------------------------------------------------------
class TableScanOperator(
    blockManager: BlockManager, 
    keyColIdx: Int, 
    valColIdx: Int,
    batchSize: Int = 4096
) extends Operator {
  
  private var blocks: Array[Long] = _
  private var currentBlockIdx = 0
  private var currentRowInBlock = 0
  private var batch: VectorBatch = _

  override def open(): Unit = {
    blocks = blockManager.getLoadedBlocks.toArray
    currentBlockIdx = 0
    currentRowInBlock = 0
    batch = new VectorBatch(batchSize)
  }

  // [SMART READER] Handles compression dispatch
  private def readColumnChunk(
      blockPtr: Long, 
      colIdx: Int, 
      startRow: Int, 
      count: Int, 
      dstPtr: Long, 
      dstOffsetBytes: Long
  ): Unit = {
    // 1. Get Stride
    var stride = NativeBridge.getColumnStride(blockPtr, colIdx)
    
    // [CRITICAL FIX] Safety net for uninitialized blocks (stride=0)
    // If stride is 0, we assume standard 4-byte Integers.
    if (stride == 0) stride = 4
    
    val colBase = NativeBridge.getColumnPtr(blockPtr, colIdx)
    
    // 2. Calculate Source Pointer
    val srcOffset = startRow.toLong * stride.toLong
    val srcPtr = NativeBridge.getOffsetPointer(colBase, srcOffset)
    
    // 3. Calculate Dest Pointer
    val finalDstPtr = NativeBridge.getOffsetPointer(dstPtr, dstOffsetBytes)
    
    // 4. Dispatch
    stride match {
      case 4 => 
        NativeBridge.memcpy(srcPtr, finalDstPtr, count * 4L)
      case 1 =>
        NativeBridge.unpack8To32(srcPtr, finalDstPtr, count)
      case 2 =>
        NativeBridge.unpack16To32(srcPtr, finalDstPtr, count)
      case _ =>
        NativeBridge.memcpy(srcPtr, finalDstPtr, count * 4L)
    }
  }

  override def next(): VectorBatch = {
    if (currentBlockIdx >= blocks.length) return null

    var rowsFilled = 0
    
    while (rowsFilled < batchSize && currentBlockIdx < blocks.length) {
      val blockPtr = blocks(currentBlockIdx)
      
      if (blockPtr == 0) {
        currentBlockIdx += 1
        currentRowInBlock = 0
      } else {
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
    batch
  }

  override def close(): Unit = {
    if (batch != null) batch.close()
  }
}