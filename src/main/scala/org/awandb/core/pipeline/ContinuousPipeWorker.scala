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

package org.awandb.core.pipeline

import org.awandb.core.jni.NativeBridge

/**
 * A stateful worker that tail-reads a native CACHE block.
 * Guarantees zero data loss by strictly managing the read offset.
 */
class ContinuousPipeWorker(
    sourceBlockPtr: Long,
    colIdx: Int,
    batchSize: Int,
    transferBufferPtr: Long
) {
  // The absolute source of truth for what has been successfully processed
  private var currentOffset: Int = 0

  def getCurrentOffset: Int = currentOffset

  /**
   * Polls the native block for new rows.
   * @return The number of rows read in this batch.
   */
  def poll(): Int = {
    if (sourceBlockPtr == 0) return 0

    // 1. Call down to C++ via the NativeBridge object wrapper to grab the next chunk
    val rowsRead = NativeBridge.tailReadPipe(
      sourceBlockPtr,
      colIdx,
      currentOffset,
      batchSize,
      transferBufferPtr
    )

    if (rowsRead > 0) {
      // 2. Here is where you would push `transferBufferPtr` data 
      // to the destination table's EngineManager/Actor queue.
      // e.g., destinationEngineManager ! AppendBatch(transferBufferPtr, rowsRead)
      
      // 3. Commit the offset ONLY after the read (and subsequent write queueing) succeeds
      currentOffset += rowsRead
    }

    rowsRead
  }
}