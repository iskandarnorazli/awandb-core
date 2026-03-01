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
// THE DATA UNIT (VectorBatch)
// -------------------------------------------------------------------------
class VectorBatch(val capacity: Int, val valueWidthBytes: Int = 4) {
  var count: Int = 0
  
  // Column 1: Keys are always 32-bit Ints
  val keysPtr: Long = NativeBridge.allocMainStore(capacity) 
  
  // Column 2: Values (4 bytes for Ints, 8 bytes for Longs)
  val valuesPtr: Long = NativeBridge.allocMainStore(capacity * (valueWidthBytes / 4)) 
  
  // [LATE MATERIALIZATION METADATA]
  
  // 1. Selection Vector: List of 32-bit Row IDs (indices) that survived filters/joins.
  val selectionVectorPtr: Long = NativeBridge.allocMainStore(capacity)
  
  // 2. Source Block: Pointer to the memory block these Row IDs belong to.
  var blockPtr: Long = 0
  
  // 3. [CRITICAL FIX] Start Row Offset
  // The row index within the block where this batch begins.
  // e.g., if this batch processes rows 4096 to 8191, this will be 4096.
  // MaterializeOperator uses this to offset the base pointer.
  var startRowInBlock: Int = 0
  
  // 4. Flag: Does this batch contain a valid Selection Vector?
  var hasSelection: Boolean = false

  def close(): Unit = {
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valuesPtr)
    NativeBridge.freeMainStore(selectionVectorPtr)
  }
}

trait Operator {
  def open(): Unit
  def next(): VectorBatch 
  def close(): Unit
}

// -------------------------------------------------------------------------
// 1. SCAN OPERATOR (Memory Source)
// -------------------------------------------------------------------------
class ScanOperator(data: Array[Int], batchSize: Int) extends Operator {
  private var pos = 0
  private var batch: VectorBatch = _

  override def open(): Unit = batch = new VectorBatch(batchSize)
  override def next(): VectorBatch = {
    if (pos >= data.length) return null
    val len = math.min(data.length - pos, batch.capacity)
    NativeBridge.loadData(batch.keysPtr, data.slice(pos, pos + len))
    batch.count = len
    pos += len
    batch
  }
  override def close(): Unit = if (batch != null) batch.close()
}

// -------------------------------------------------------------------------
// 3. SIP FILTER OPERATOR (Transform)
// -------------------------------------------------------------------------
class SipFilterOperator(child: Operator, cuckooPtr: Long) extends Operator {
  private var tempBitmaskPtr: Long = 0

  override def open(): Unit = {
    child.open()
    tempBitmaskPtr = NativeBridge.allocMainStore(4096) // Match standard batch size
  }

  override def next(): VectorBatch = {
    var batch = child.next()
    while (batch != null && batch.count > 0) {
      if (cuckooPtr != 0L) {
        // 1. Natively Probe Cuckoo Filter
        NativeBridge.cuckooProbeBatch(cuckooPtr, batch.keysPtr, batch.count, tempBitmaskPtr)

        val bitmask = new Array[Int](batch.count)
        NativeBridge.copyToScala(tempBitmaskPtr, bitmask, batch.count)

        val keys = new Array[Int](batch.count)
        val vals = new Array[Int](batch.count)
        NativeBridge.copyToScala(batch.keysPtr, keys, batch.count)
        NativeBridge.copyToScala(batch.valuesPtr, vals, batch.count)

        // Prep the Selection Vector to maintain Late Materialization integrity
        val originalIndices = new Array[Int](batch.count)
        val inputSel = if (batch.hasSelection) {
            val arr = new Array[Int](batch.count)
            NativeBridge.copyToScala(batch.selectionVectorPtr, arr, batch.count)
            arr
        } else null

        // 2. Compact the batch
        var keepCount = 0
        var i = 0
        while (i < batch.count) {
          if (bitmask(i) == 1) {
            keys(keepCount) = keys(i)
            vals(keepCount) = vals(i)
            originalIndices(keepCount) = if (batch.hasSelection) inputSel(i) else i
            keepCount += 1
          }
          i += 1
        }

        if (keepCount > 0) {
           NativeBridge.loadData(batch.keysPtr, keys)
           NativeBridge.loadData(batch.valuesPtr, vals)
           NativeBridge.loadData(batch.selectionVectorPtr, originalIndices)
           batch.hasSelection = true 
           batch.count = keepCount
           return batch
        } else {
           // Entire batch was filtered out (Massive IO savings for Materialize)!
           batch = child.next()
        }
      } else {
        return batch
      }
    }
    null
  }

  override def close(): Unit = {
    child.close()
    if (tempBitmaskPtr != 0L) NativeBridge.freeMainStore(tempBitmaskPtr)
  }
}

// -------------------------------------------------------------------------
// 4. SORT OPERATOR (Blocking)
// -------------------------------------------------------------------------
class SortOperator(child: Operator, rowCountHint: Int) extends Operator {
  private var dataPtr: Long = 0
  private var count = 0
  private var outputEmitted = false
  private var resultBatch: VectorBatch = _

  override def open(): Unit = {
    child.open()
    dataPtr = NativeBridge.allocMainStore(rowCountHint)
    resultBatch = new VectorBatch(1024) 
  }
  override def next(): VectorBatch = {
    if (outputEmitted) return null
    var batch = child.next()
    while (batch != null) {
      val offset = count * 4L
      val dst = NativeBridge.getOffsetPointer(dataPtr, offset)
      NativeBridge.memcpy(batch.keysPtr, dst, batch.count * 4L)
      count += batch.count
      batch = child.next()
    }
    if (count > 0) NativeBridge.radixSort(dataPtr, count)
    outputEmitted = true
    resultBatch.count = count 
    resultBatch
  }
  override def close(): Unit = {
    child.close()
    if (dataPtr != 0) NativeBridge.freeMainStore(dataPtr)
    if (resultBatch != null) resultBatch.close()
  }
}

// -------------------------------------------------------------------------
// 5. AGGREGATION OPERATOR (Blocking)
// -------------------------------------------------------------------------
class HashAggOperator(child: Operator) extends Operator {
  private var mapPtr: Long = 0
  private var outputEmitted = false
  private var resultBatch: VectorBatch = _
  
  private var tempKeys: Long = 0
  private var tempVals: Long = 0
  private var totalInput = 0
  private val MAX_INPUT = 10_000_000 

  override def open(): Unit = {
    child.open()
    tempKeys = NativeBridge.allocMainStore(MAX_INPUT)
    tempVals = NativeBridge.allocMainStore(MAX_INPUT)
  }

  override def next(): VectorBatch = {
    if (outputEmitted) return null

    var batch = child.next()
    while (batch != null && totalInput < MAX_INPUT) {
      val offsetBytes = totalInput * 4L
      NativeBridge.memcpy(batch.keysPtr, NativeBridge.getOffsetPointer(tempKeys, offsetBytes), batch.count * 4L)
      NativeBridge.memcpy(batch.valuesPtr, NativeBridge.getOffsetPointer(tempVals, offsetBytes), batch.count * 4L)
      totalInput += batch.count
      batch = child.next()
    }

    if (totalInput > 0) {
        mapPtr = NativeBridge.aggregateSum(tempKeys, tempVals, totalInput)
        
        // [DEBUG] Prove allocation size
        println(s"[HashAgg] Allocating ResultBatch for $totalInput items with WIDTH = 8 bytes (Long)")
        
        // [CRITICAL FIX] valueWidthBytes = 8 is MANDATORY
        resultBatch = new VectorBatch(totalInput, valueWidthBytes = 8) 
        
        val resultCount = NativeBridge.aggregateExport(mapPtr, resultBatch.keysPtr, resultBatch.valuesPtr)
        println(s"[HashAgg] Exported $resultCount groups.")
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
  }
}