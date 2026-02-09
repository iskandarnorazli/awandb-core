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

// -------------------------------------------------------------------------
// MATERIALIZE OPERATOR (Late Fetch)
// Fetches a column using Row IDs from the Selection Vector.
// -------------------------------------------------------------------------
class MaterializeOperator(
    child: Operator, 
    colIdx: Int // The column index within the source block
) extends Operator {
  
  override def open(): Unit = child.open()

  override def next(): VectorBatch = {
    val batch = child.next()
    if (batch == null) return null
    
    // IF we have Row IDs and a Source Block -> Fetch Data
    if (batch.hasSelection && batch.blockPtr != 0) {
        
        // 1. Get pointer to the column we want
        val colBasePtr = NativeBridge.getColumnPtr(batch.blockPtr, colIdx)
        
        if (colBasePtr != 0) {
            // [CRITICAL FIX] Use IntToLong gather because Source is Int(4B) but Dest is Long(8B)
            NativeBridge.batchReadIntToLong(
                colBasePtr, 
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