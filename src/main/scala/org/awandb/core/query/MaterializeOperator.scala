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

class MaterializeOperator(
    child: Operator, 
    colIdx: Int 
) extends Operator {
  
  override def open(): Unit = child.open()

  override def next(): VectorBatch = {
    val batch = child.next()
    if (batch == null) return null
    
    if (batch.hasSelection && batch.blockPtr != 0) {
        
        // 1. Get Base Pointer of the block
        val colBasePtr = NativeBridge.getColumnPtr(batch.blockPtr, colIdx)
        
        if (colBasePtr != 0) {
            // [CRITICAL FIX] Apply the Batch Offset!
            // The batch contains indices 0..N, but these correspond to rows (Start + 0)..(Start + N)
            // So we advance the pointer to the start of this batch's range in the block.
            
            // Assume Stride is 4 (Int)
            // TODO: In a real engine, resolve stride from header. 
            // For now, we assume integers as per spec.
            val offsetBytes = batch.startRowInBlock * 4L 
            val chunkPtr = NativeBridge.getOffsetPointer(colBasePtr, offsetBytes)
            
            // 2. Gather from the offset position
            NativeBridge.batchReadIntToLong(
                chunkPtr, // Read from here
                batch.selectionVectorPtr, 
                batch.count, 
                batch.valuesPtr 
            )
        }
    }
    
    batch
  }

  override def close(): Unit = child.close()
}