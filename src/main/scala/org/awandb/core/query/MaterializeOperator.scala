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
        val colBasePtr = NativeBridge.getColumnPtr(batch.blockPtr, colIdx)
        
        if (colBasePtr != 0) {
            // [CRITICAL FIX 1] Restore the offset! selectionVector is batch-local (0 to count-1)
            val offsetBytes = batch.startRowInBlock * 4L 
            val chunkPtr = NativeBridge.getOffsetPointer(colBasePtr, offsetBytes)
            
            // [CRITICAL FIX 2] Dynamic Memory Width check to prevent Segfaults & Corruption!
            if (batch.valueWidthBytes == 8) {
                // Downstream (e.g., HashAgg) allocated a 64-bit buffer. Widen the ints.
                NativeBridge.batchReadIntToLong(chunkPtr, batch.selectionVectorPtr, batch.count, batch.valuesPtr)
            } else {
                // Downstream allocated a 32-bit buffer. Do a direct 1:1 copy.
                NativeBridge.batchRead(chunkPtr, batch.selectionVectorPtr, batch.count, batch.valuesPtr)
            }
        }
    }
    
    batch
  }

  override def close(): Unit = child.close()
}