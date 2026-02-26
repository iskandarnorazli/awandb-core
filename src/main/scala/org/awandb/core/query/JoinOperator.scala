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
// Reads the RIGHT table and builds a Hash Map.
// -------------------------------------------------------------------------
class HashJoinBuildOperator(child: Operator) extends Operator {
  var mapPtr: Long = 0L
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
    // 1. Consume ALL input from the Right table
    var batch = child.next()
    while (batch != null && totalInput < MAX_BUILD_SIZE) {
      val offset = totalInput * 4L // offset for keys (4 bytes)
      val payloadOffset = totalInput * 8L // offset for payloads (8 bytes)
      
      // Copy Keys: Batch -> TempBuffer
      NativeBridge.memcpy(batch.keysPtr, NativeBridge.getOffsetPointer(tempKeys, offset), batch.count * 4L)
      
      // [FIX] Widen 32-bit Payloads to 64-bit for the Hash Map
      if (batch.valueWidthBytes == 4) {
         val ints = new Array[Int](batch.count)
         NativeBridge.copyToScala(batch.valuesPtr, ints, batch.count)
         
         // 2 ints = 1 Long (Little Endian memory layout)
         val widened = new Array[Int](batch.count * 2)
         var i = 0
         while (i < batch.count) {
            widened(i * 2) = ints(i) // Lower 32 bits
            widened(i * 2 + 1) = if (ints(i) < 0) -1 else 0 // Upper 32 bits (Sign extension)
            i += 1
         }
         NativeBridge.loadData(NativeBridge.getOffsetPointer(tempPayloads, payloadOffset), widened)
      } else {
         NativeBridge.memcpy(batch.valuesPtr, NativeBridge.getOffsetPointer(tempPayloads, payloadOffset), batch.count * 8L)
      }
      
      totalInput += batch.count
      batch = child.next()
    }

    // 2. Build the C++ Hash Map
    if (totalInput > 0) {
      mapPtr = NativeBridge.joinBuild(tempKeys, tempPayloads, totalInput)
    }
    
    // Build Operator returns nothing (it is a sink for the pipeline)
    null 
  }
  
  def getMapPtr(): Long = mapPtr

  override def close(): Unit = {
    child.close()
    if (tempKeys != 0) NativeBridge.freeMainStore(tempKeys)
    if (tempPayloads != 0) NativeBridge.freeMainStore(tempPayloads)
    // Don't free mapPtr here! It's needed by Probe.
  }
  
  def destroyMap(): Unit = {
    if (mapPtr != 0) {
      NativeBridge.joinDestroy(mapPtr)
      mapPtr = 0
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
  // Buffer to hold indices of matching probe rows (allocated once)
  private var matchIndicesPtr: Long = 0 

  override def open(): Unit = {
    // Ensure Build side is ready
    if (buildOp.getMapPtr() == 0) {
       buildOp.open()
       buildOp.next() // Triggers the build loop
    }
    
    mapPtr = buildOp.getMapPtr()
    if (mapPtr == 0) throw new RuntimeException("Join Build Failed: Map is null")
    
    child.open()
    outBatch = new VectorBatch(4096, valueWidthBytes = 8) // Output: Key (Int) + Payload (Long)
    matchIndicesPtr = NativeBridge.allocMainStore(4096)
  }

  override def next(): VectorBatch = {
    var inputBatch = child.next()
    
    // Loop until we find matches or run out of input
    while (inputBatch != null) {
      
      // Probe C++ Map
      // keysPtr = Probe Keys from Input
      // valuesPtr = Output buffer for Payloads
      // matchIndicesPtr = Output buffer for Row IDs (indices in inputBatch)
      val matches = NativeBridge.joinProbe(
          mapPtr, 
          inputBatch.keysPtr, 
          inputBatch.count, 
          outBatch.valuesPtr, 
          matchIndicesPtr
      )
      
      if (matches > 0) {
        outBatch.count = matches
        
        // [LATE MATERIALIZATION SUPPORT]
        // 1. Pass the Source Block Pointer downstream
        outBatch.blockPtr = inputBatch.blockPtr
        
        // [CRITICAL FIX] Pass the Start Row Offset downstream
        // This ensures MaterializeOperator knows where the batch starts relative to the block
        outBatch.startRowInBlock = inputBatch.startRowInBlock
        
        outBatch.hasSelection = true
        
        // 2. Copy the Match Indices into the Selection Vector
        // This allows MaterializeOperator to know exactly which rows to fetch
        NativeBridge.memcpy(matchIndicesPtr, outBatch.selectionVectorPtr, matches * 4L)
        
        // 3. Gather Keys for the Result Batch (completeness)
        // We fetch the Keys from the Input using the indices we just got
        NativeBridge.batchRead(inputBatch.keysPtr, matchIndicesPtr, matches, outBatch.keysPtr)
        
        return outBatch
      }
      
      // No matches in this batch, try next
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