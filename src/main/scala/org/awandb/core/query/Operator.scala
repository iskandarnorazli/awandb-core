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

// THE DATA UNIT (VectorBatch)
// Holds a chunk of data (e.g., 4096 rows) to keep CPU caches hot.
class VectorBatch(val capacity: Int) {
  var count: Int = 0
  val keysPtr: Long = NativeBridge.allocMainStore(capacity) // Data
  val selectionPtr: Long = NativeBridge.allocMainStore(capacity) // Filter results (0 or 1)
  
  def close(): Unit = {
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(selectionPtr)
  }
}

// THE BASE OPERATOR (Volcano Model)
trait Operator {
  def open(): Unit
  def next(): VectorBatch // Returns null when finished
  def close(): Unit
}

// 1. SCAN OPERATOR (Source)
// Reads raw integers from a source array into batches
class ScanOperator(data: Array[Int], batchSize: Int) extends Operator {
  private var pos = 0
  private var batch: VectorBatch = _

  override def open(): Unit = {
    batch = new VectorBatch(batchSize)
  }

  override def next(): VectorBatch = {
    if (pos >= data.length) return null
    
    val remaining = data.length - pos
    val len = math.min(remaining, batch.capacity)
    
    // Simulate reading a Block from disk/memory
    // We copy a slice of the array into the Native Buffer
    val slice = data.slice(pos, pos + len)
    NativeBridge.loadData(batch.keysPtr, slice)
    
    batch.count = len
    pos += len
    batch
  }

  override def close(): Unit = batch.close()
}

// 2. SIP FILTER OPERATOR (Transform)
// Applies Cuckoo Filter to the batch. 
// Uses "Selection Vectors" (marking rows as invalid) rather than copying data.
class SipFilterOperator(child: Operator, cuckooPtr: Long) extends Operator {
  
  override def open(): Unit = child.open()

  override def next(): VectorBatch = {
    var batch = child.next()
    
    // Loop until we find a batch with matches or run out of data
    while (batch != null) {
      
      // VECTORIZED PROBE: 
      // 1 JNI call processes 4096 rows. 
      // Result is written to 'selectionPtr' (byte array of 0s and 1s)
      NativeBridge.cuckooProbeBatch(cuckooPtr, batch.keysPtr, batch.count, batch.selectionPtr)
      
      // In a full engine, we would 'compact' the batch here to remove 0s.
      // For this benchmark, we just return the batch marked with hits/misses.
      return batch
    }
    null
  }

  override def close(): Unit = child.close()
}