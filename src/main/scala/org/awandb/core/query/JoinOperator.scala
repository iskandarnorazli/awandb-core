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
// 1. BUILD OPERATOR (Blocking)
// Reads the RIGHT table, builds a Hash Map, AND builds the SIP Cuckoo Filter.
// -------------------------------------------------------------------------
class HashJoinBuildOperator(child: Operator) extends Operator {
  var mapPtr: Long = 0L
  var cuckooPtr: Long = 0L // [NEW] SIP Filter
  
  private var tempKeys: Long = 0
  private var tempPayloads: Long = 0
  private var totalInput = 0
  private val MAX_BUILD_SIZE = 10_000_000 

  override def open(): Unit = {
    child.open()
    tempKeys = NativeBridge.allocMainStore(MAX_BUILD_SIZE)
    // Payloads are 64-bit (Long), so allocate double space
    tempPayloads = NativeBridge.allocMainStore(MAX_BUILD_SIZE * 2) 
  }

  override def next(): VectorBatch = {
    var batch = child.next()
    while (batch != null && totalInput < MAX_BUILD_SIZE) {
      val offset = totalInput * 4L 
      val payloadOffset = totalInput * 8L 
      
      NativeBridge.memcpy(batch.keysPtr, NativeBridge.getOffsetPointer(tempKeys, offset), batch.count * 4L)
      
      if (batch.valueWidthBytes == 4) {
         val ints = new Array[Int](batch.count)
         NativeBridge.copyToScala(batch.valuesPtr, ints, batch.count)
         val widened = new Array[Int](batch.count * 2)
         var i = 0
         while (i < batch.count) {
            widened(i * 2) = ints(i) 
            widened(i * 2 + 1) = if (ints(i) < 0) -1 else 0 
            i += 1
         }
         NativeBridge.loadData(NativeBridge.getOffsetPointer(tempPayloads, payloadOffset), widened)
      } else {
         NativeBridge.memcpy(batch.valuesPtr, NativeBridge.getOffsetPointer(tempPayloads, payloadOffset), batch.count * 8L)
      }
      
      totalInput += batch.count
      batch = child.next()
    }

    if (totalInput > 0) {
      // 1. Build the exact Hash Map for the Join
      mapPtr = NativeBridge.joinBuild(tempKeys, tempPayloads, totalInput)
      
      // 2. [SIP] Build the approximate Cuckoo Filter for pushdown (1.5x capacity)
      cuckooPtr = NativeBridge.cuckooCreate(math.max((totalInput * 1.5).toInt, 1024))
      val scalaKeys = new Array[Int](totalInput)
      NativeBridge.copyToScala(tempKeys, scalaKeys, totalInput)
      NativeBridge.cuckooBuildBatch(cuckooPtr, scalaKeys)
    }
    null 
  }
  
  def getMapPtr(): Long = mapPtr
  def getCuckooPtr(): Long = cuckooPtr

  override def close(): Unit = {
    child.close()
    if (tempKeys != 0) NativeBridge.freeMainStore(tempKeys)
    if (tempPayloads != 0) NativeBridge.freeMainStore(tempPayloads)
  }
  
  def destroyMap(): Unit = {
    if (mapPtr != 0) {
      NativeBridge.joinDestroy(mapPtr)
      mapPtr = 0
    }
    if (cuckooPtr != 0) {
      NativeBridge.cuckooDestroy(cuckooPtr)
      cuckooPtr = 0
    }
  }
}

// -------------------------------------------------------------------------
// 2. PROBE OPERATOR (Streaming)
// Reads the LEFT table and probes the Hash Map.
// Output: Join Keys + Payloads + Selection Vector (Row IDs for Late Materialization)
// -------------------------------------------------------------------------
class HashJoinProbeOperator(child: Operator, buildOp: HashJoinBuildOperator) extends Operator {
  private var mapPtr: Long = 0
  private var outBatch: VectorBatch = _
  private var matchIndicesPtr: Long = 0 

  override def open(): Unit = {
    if (buildOp.getMapPtr() == 0) {
       buildOp.open()
       buildOp.next() 
    }
    
    mapPtr = buildOp.getMapPtr()
    if (mapPtr == 0) throw new RuntimeException("Join Build Failed: Map is null")
    
    child.open()
    outBatch = new VectorBatch(4096, valueWidthBytes = 8) 
    matchIndicesPtr = NativeBridge.allocMainStore(4096)
  }

  override def next(): VectorBatch = {
    var inputBatch = child.next()
    
    while (inputBatch != null) {
      val matches = NativeBridge.joinProbe(
          mapPtr, 
          inputBatch.keysPtr, 
          inputBatch.count, 
          outBatch.valuesPtr, 
          matchIndicesPtr
      )
      
      if (matches > 0) {
        outBatch.count = matches
        outBatch.blockPtr = inputBatch.blockPtr
        outBatch.startRowInBlock = inputBatch.startRowInBlock
        outBatch.hasSelection = true
        
        // [CRITICAL FIX] Map the Selection Vector so downstream Materialize Operator 
        // doesn't read corrupted offsets if SIP dropped rows earlier.
        if (inputBatch.hasSelection) {
            NativeBridge.batchRead(inputBatch.selectionVectorPtr, matchIndicesPtr, matches, outBatch.selectionVectorPtr)
        } else {
            NativeBridge.memcpy(matchIndicesPtr, outBatch.selectionVectorPtr, matches * 4L)
        }
        
        NativeBridge.batchRead(inputBatch.keysPtr, matchIndicesPtr, matches, outBatch.keysPtr)
        return outBatch
      }
      inputBatch = child.next()
    }
    null
  }

  override def close(): Unit = {
    child.close()
    if (outBatch != null) outBatch.close()
    if (matchIndicesPtr != 0) NativeBridge.freeMainStore(matchIndicesPtr)
    buildOp.destroyMap()
    buildOp.close()
  }
}