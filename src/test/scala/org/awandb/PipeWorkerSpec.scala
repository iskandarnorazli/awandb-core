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

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.jni.NativeBridge
import org.awandb.core.pipeline.ContinuousPipeWorker // Fixed missing import

class PipeWorkerSpec extends AnyFunSuite {

  test("PipeWorker should exactly tail-read CACHE block and transfer to NORMAL block") {
    val numRows = 100
    val batchSize = 40
    
    // 1. Setup Source (CACHE) block
    // Simulating 1 column of TYPE_INT
    val sourceBlockPtr = NativeBridge.createBlock(numRows, 1) 
    
    // Allocate temporary native memory for the Pipe to read into
    val transferBufferPtr = NativeBridge.allocMainStore(batchSize) 

    // 2. Simulate Ingestion into CACHE table
    val sourceData = (1 to numRows).toArray
    val sourceColPtr = NativeBridge.getColumnPtr(sourceBlockPtr, 0)
    NativeBridge.loadData(sourceColPtr, sourceData) // Removed "Native" suffix
    
    // Artificially update the row_count in the C++ BlockHeader to simulate writes
    NativeBridge.setRowCount(sourceBlockPtr, numRows) // Removed "Native" suffix

    // 3. Initialize the Pipe Worker
    val pipe = new ContinuousPipeWorker(
      sourceBlockPtr = sourceBlockPtr,
      colIdx = 0,
      batchSize = batchSize,
      transferBufferPtr = transferBufferPtr
    )

    // 4. Poll 1: Should read first 40 rows
    val read1 = pipe.poll()
    assert(read1 == 40)
    assert(pipe.getCurrentOffset == 40)
    
    // Verify data in transfer buffer (simulating the write to destination)
    val scalaBuf1 = new Array[Int](40)
    NativeBridge.copyToScala(transferBufferPtr, scalaBuf1, 40) // Removed "Native" suffix
    assert(scalaBuf1.head == 1 && scalaBuf1.last == 40)

    // 5. Poll 2 & 3: Read remaining rows
    val read2 = pipe.poll()
    assert(read2 == 40)
    assert(pipe.getCurrentOffset == 80)

    val read3 = pipe.poll()
    assert(read3 == 20) // Only 20 left
    assert(pipe.getCurrentOffset == 100)

    // 6. Poll 4: Should return 0 (no new data)
    val read4 = pipe.poll()
    assert(read4 == 0)
    assert(pipe.getCurrentOffset == 100)

    // Cleanup
    NativeBridge.freeMainStore(transferBufferPtr) // Removed "Native" suffix
    NativeBridge.freeMainStore(sourceBlockPtr)    // Replaced freeBlockNative with freeMainStore
  }

  test("PipeWorker should handle continuous dynamic ingestion (simulated concurrent appends)") {
    val blockCapacity = 100
    val batchSize = 25
    
    val sourceBlockPtr = NativeBridge.createBlock(blockCapacity, 1)
    val transferBufferPtr = NativeBridge.allocMainStore(batchSize)
    
    // Pre-load data into memory, but control visibility via row_count to simulate a live writer
    val sourceData = (1 to blockCapacity).toArray
    val sourceColPtr = NativeBridge.getColumnPtr(sourceBlockPtr, 0)
    NativeBridge.loadData(sourceColPtr, sourceData)
    
    val pipe = new ContinuousPipeWorker(
      sourceBlockPtr = sourceBlockPtr,
      colIdx = 0,
      batchSize = batchSize,
      transferBufferPtr = transferBufferPtr
    )
    
    // Step 1: Writer appends first 15 rows
    NativeBridge.setRowCount(sourceBlockPtr, 15)
    assert(pipe.poll() == 15) // Reads all 15 available
    assert(pipe.getCurrentOffset == 15)
    assert(pipe.poll() == 0)  // No more new data to read
    
    // Step 2: Writer appends up to row 45 (30 new rows total)
    NativeBridge.setRowCount(sourceBlockPtr, 45)
    
    // Reader hits the batch limit (25) first
    assert(pipe.poll() == 25) 
    assert(pipe.getCurrentOffset == 40)
    
    // Reader grabs the remaining 5 on the next poll
    assert(pipe.poll() == 5)  
    assert(pipe.getCurrentOffset == 45)
    
    // Verify the data in the last chunk (should be values 41 to 45)
    val scalaBuf = new Array[Int](5)
    NativeBridge.copyToScala(transferBufferPtr, scalaBuf, 5)
    assert(scalaBuf.toSeq == Seq(41, 42, 43, 44, 45))
    
    // Cleanup
    NativeBridge.freeMainStore(transferBufferPtr)
    NativeBridge.freeMainStore(sourceBlockPtr)
  }

  test("PipeWorker should safely handle edge cases (empty blocks and null pointers)") {
    // 1. Graceful handling of Null Pointers
    val nullPipe = new ContinuousPipeWorker(
      sourceBlockPtr = 0L, 
      colIdx = 0, 
      batchSize = 10, 
      transferBufferPtr = 0L
    )
    assert(nullPipe.poll() == 0)
    assert(nullPipe.getCurrentOffset == 0)
    
    // 2. Graceful handling of Empty Blocks (no data written yet)
    val emptyBlockPtr = NativeBridge.createBlock(10, 1)
    val transferBufferPtr = NativeBridge.allocMainStore(10)
    NativeBridge.setRowCount(emptyBlockPtr, 0)
    
    val emptyPipe = new ContinuousPipeWorker(
      sourceBlockPtr = emptyBlockPtr, 
      colIdx = 0, 
      batchSize = 10, 
      transferBufferPtr = transferBufferPtr
    )
    
    assert(emptyPipe.poll() == 0)
    assert(emptyPipe.getCurrentOffset == 0)
    
    // Cleanup
    NativeBridge.freeMainStore(transferBufferPtr)
    NativeBridge.freeMainStore(emptyBlockPtr)
  }
}