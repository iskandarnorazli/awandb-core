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

import scala.collection.mutable.ArrayBuffer

class NativeColumn(val name: String, val isString: Boolean = false) {
  
  // 1. Delta Store (Write-Optimized RAM)
  // Renamed to 'deltaIntBuffer' to match AwanDBSpec expectations
  val deltaIntBuffer = new ArrayBuffer[Int]()

  /**
   * Append a value to the in-memory delta buffer.
   */
  def insert(value: Int): Unit = {
    if (isString) throw new IllegalStateException(s"Column $name is a String column. Cannot insert Int.")
    deltaIntBuffer.append(value)
  }

  /**
   * [WRITE FUSION]
   * Efficiently appends an entire array of values.
   * Under the hood, this uses System.arraycopy (memcpy), which is 
   * orders of magnitude faster than looping append().
   */
  def insertBatch(values: Array[Int]): Unit = {
    if (isString) throw new IllegalStateException(s"Column $name is a String column.")
    deltaIntBuffer ++= values
  }

  /**
   * Clear the buffer after a successful flush to disk.
   */
  def clearDelta(): Unit = {
    deltaIntBuffer.clear()
  }
  
  def isEmpty: Boolean = deltaIntBuffer.isEmpty

  /**
   * Helper to get current delta as an array (for flushing).
   */
  def toArray: Array[Int] = deltaIntBuffer.toArray
}