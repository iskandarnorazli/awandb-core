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
  // We allocate capacity * (width / 4) integers.
  // 4 bytes -> capacity * 1
  // 8 bytes -> capacity * 2
  val valuesPtr: Long = NativeBridge.allocMainStore(capacity * (valueWidthBytes / 4)) 
  
  def close(): Unit = {
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valuesPtr)
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
  override def open(): Unit = child.open()
  override def next(): VectorBatch = {
    var batch = child.next()
    while (batch != null) {
      NativeBridge.cuckooProbeBatch(cuckooPtr, batch.keysPtr, batch.count, batch.valuesPtr)
      return batch
    }
    null
  }
  override def close(): Unit = child.close()
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