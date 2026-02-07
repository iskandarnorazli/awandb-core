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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.query

import org.awandb.core.jni.NativeBridge

// THE DATA UNIT (VectorBatch)
// Holds a chunk of data (e.g., 4096 rows) to keep CPU caches hot.
class VectorBatch(val capacity: Int) {
  var count: Int = 0
  val keysPtr: Long = NativeBridge.allocMainStore(capacity) // Data
  val selectionPtr: Long = NativeBridge.allocMainStore(capacity) // Filter results (0 or 1) OR Values
  
  // Clean up manual memory
  def close(): Unit = {
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(selectionPtr)
  }
}

// THE BASE OPERATOR (Volcano Model)
trait Operator {
  def open(): Unit
  def next(): VectorBatch // Returns null if exhausted
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

  override def close(): Unit = {
    if (batch != null) batch.close()
  }
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
      
      // Ideally, we would 'compact' here to remove filtered rows.
      // For now, we return the batch with the selection vector populated.
      return batch
    }
    null
  }

  override def close(): Unit = child.close()
}

// 3. SORT OPERATOR (Blocking)
// Sorts the entire dataset. This is a "Pipeline Breaker".
class SortOperator(child: Operator, rowCountHint: Int) extends Operator {
  private var dataPtr: Long = 0
  private var count = 0
  private var outputEmitted = false
  private var resultBatch: VectorBatch = _

  override def open(): Unit = {
    child.open()
    // Alloc big buffer for sorting
    dataPtr = NativeBridge.allocMainStore(rowCountHint)
    resultBatch = new VectorBatch(1024) // Small batch wrapper for output
  }

  override def next(): VectorBatch = {
    if (outputEmitted) return null

    // 1. MATERIALIZE (Consume entire child input)
    var batch = child.next()
    
    while (batch != null) {
      // In a real engine: NativeBridge.memcpy(src=batch.keysPtr, dst=dataPtr + offset, len=...)
      // For this prototype, we simulate accumulation logic in C++ or benchmark harness.
      count += batch.count
      batch = child.next()
    }

    // 2. SORT (C++ Radix Sort)
    if (count > 0) {
        NativeBridge.radixSort(dataPtr, count)
    }
    
    outputEmitted = true
    
    // Hack: Return everything in one "virtual" batch
    resultBatch.count = count 
    // Ideally update keysPtr, but it's immutable val. 
    // We assume the caller knows 'dataPtr' contains the result for this test.
    resultBatch
  }

  override def close(): Unit = {
    child.close()
    if (dataPtr != 0) NativeBridge.freeMainStore(dataPtr)
    if (resultBatch != null) resultBatch.close()
  }
}

// 4. AGGREGATION OPERATOR (Blocking)
// Groups by Key and Sums Values.
// Uses the C++ Hash Map, then Exports the result back to arrays for the Sort operator.
class HashAggOperator(child: Operator) extends Operator {
  private var mapPtr: Long = 0
  private var outputEmitted = false
  private var resultBatch: VectorBatch = _
  
  // Temporary storage to accumulate all child data before aggregation
  private var tempKeys: Long = 0
  private var tempVals: Long = 0
  private var totalInput = 0
  private val MAX_INPUT = 10_000_000 // Fixed buffer limit for prototype

  override def open(): Unit = {
    child.open()
    tempKeys = NativeBridge.allocMainStore(MAX_INPUT)
    tempVals = NativeBridge.allocMainStore(MAX_INPUT)
  }

  override def next(): VectorBatch = {
    if (outputEmitted) return null

    // 1. MATERIALIZE INPUT (Accumulate all batches)
    var batch = child.next()
    
    while (batch != null && totalInput < MAX_INPUT) {
      // Calculate the memory offset for the current chunk (4 bytes per integer)
      val offsetBytes = totalInput * 4L
      
      // Get pointers to the write position in our large temp buffers
      val dstKeyPtr = NativeBridge.getOffsetPointer(tempKeys, offsetBytes)
      val dstValPtr = NativeBridge.getOffsetPointer(tempVals, offsetBytes)
      
      // [FIX] Perform Native Memory Copy (No JVM Array Overhead)
      // Copy Keys from Batch -> Temp Buffer
      NativeBridge.memcpy(batch.keysPtr, dstKeyPtr, batch.count * 4L)
      
      // Copy Values from Batch -> Temp Buffer
      // (We assume selectionPtr holds the values to sum, e.g., 1s for COUNT or actual values)
      NativeBridge.memcpy(batch.selectionPtr, dstValPtr, batch.count * 4L)
      
      totalInput += batch.count
      batch = child.next()
    }

    // 2. AGGREGATE (Run C++ Hash Map on the accumulated data)
    if (totalInput > 0) {
        // Pass the populated temp buffers to C++
        mapPtr = NativeBridge.aggregateSum(tempKeys, tempVals, totalInput)
        
        // 3. EXPORT (Flatten Hash Map -> Result Arrays)
        // Allocate a result batch large enough for the worst case (all keys unique)
        resultBatch = new VectorBatch(totalInput) 
        
        // C++ exports the map keys/values directly into the result batch's pointers
        val resultCount = NativeBridge.aggregateExport(mapPtr, resultBatch.keysPtr, resultBatch.selectionPtr)
        resultBatch.count = resultCount
    } else {
        resultBatch = new VectorBatch(1024)
        resultBatch.count = 0
    }
    
    outputEmitted = true
    resultBatch
  }

  override def close(): Unit = {
    child.close()
    if (mapPtr != 0) NativeBridge.freeAggregationResult(mapPtr)
    if (tempKeys != 0) NativeBridge.freeMainStore(tempKeys)
    if (tempVals != 0) NativeBridge.freeMainStore(tempVals)
    // resultBatch is passed downstream, ideally closed by consumer
  }
}