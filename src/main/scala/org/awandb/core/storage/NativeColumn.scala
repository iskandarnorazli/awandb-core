/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.storage

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// [FIX] Define Block wrapper so we can track native pointers
case class Block(ptr: Long, rowCount: Int)

class NativeColumn(val name: String, val isString: Boolean = false) {
  
  // 1. Delta Store (Write-Optimized RAM)
  // We maintain two separate buffers to avoid Boxing/Unboxing overhead.
  val deltaIntBuffer = new ArrayBuffer[Int]()
  val deltaStringBuffer = new ArrayBuffer[String]()

  // 2. Snapshot Store (Read-Optimized Disk/Native RAM)
  // [FIX] Added this field so AwanTable and Tests can track flushed blocks
  val snapshotBlocks = new ListBuffer[Block]()

  // -----------------------------------------------------------
  // INSERT API
  // -----------------------------------------------------------

  /**
   * Append an Integer. Throws if this is a String column.
   */
  def insert(value: Int): Unit = {
    if (isString) throw new IllegalStateException(s"Column $name is a String column. Cannot insert Int.")
    deltaIntBuffer.append(value)
  }

  /**
   * [NEW] Append a String. Throws if this is an Integer column.
   */
  def insert(value: String): Unit = {
    if (!isString) throw new IllegalStateException(s"Column $name is an Int column. Cannot insert String.")
    deltaStringBuffer.append(value)
  }

  /**
   * [WRITE FUSION] Integer Batch
   */
  def insertBatch(values: Array[Int]): Unit = {
    if (isString) throw new IllegalStateException(s"Column $name is a String column.")
    deltaIntBuffer ++= values
  }

  /**
   * [NEW] [WRITE FUSION] String Batch
   */
  def insertBatch(values: Array[String]): Unit = {
    if (!isString) throw new IllegalStateException(s"Column $name is an Int column.")
    deltaStringBuffer ++= values
  }

  // -----------------------------------------------------------
  // LIFECYCLE API
  // -----------------------------------------------------------

  /**
   * Clear buffers after a successful flush to disk.
   */
  def clearDelta(): Unit = {
    deltaIntBuffer.clear()
    deltaStringBuffer.clear()
  }
  
  /**
   * Check if the active buffer is empty.
   */
  def isEmpty: Boolean = {
    if (isString) deltaStringBuffer.isEmpty else deltaIntBuffer.isEmpty
  }

  /**
   * Closes all native blocks associated with this column.
   * [CRITICAL] Prevents memory leaks when dropping a table.
   */
  def close(): Unit = {
    // Ideally, we call NativeBridge.freeBlock(b.ptr) here
    // For now, we clear the list to drop references
    snapshotBlocks.clear()
    deltaIntBuffer.clear()
    deltaStringBuffer.clear()
  }

  // ==============================================================================
  // HELPER FUNCTIONS (Critical for JNI Flush)
  // ==============================================================================

  /**
   * Helper to get current delta as an Int array (for flushing to NativeBridge).
   */
  def toIntArray: Array[Int] = deltaIntBuffer.toArray
  
  /**
   * [NEW] Helper to get current delta as a String array.
   * AwanTable calls this to pass data to NativeBridge.loadStringDataNative()
   */
  def toStringArray: Array[String] = deltaStringBuffer.toArray
  
  /**
   * Legacy compatibility (optional, but good for existing tests).
   */
  def toArray: Array[Int] = toIntArray
}