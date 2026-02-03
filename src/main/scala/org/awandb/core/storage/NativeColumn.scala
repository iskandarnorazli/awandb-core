/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.storage

import scala.collection.mutable.ArrayBuffer
import org.awandb.core.jni.NativeBridge

class NativeColumn(val name: String) {
  
  // 1. Delta Store (Write-Optimized RAM)
  // New inserts go here until flush()
  val deltaBuffer = new ArrayBuffer[Int]()

  /**
   * Append a value to the in-memory delta buffer.
   */
  def insert(value: Int): Unit = {
    deltaBuffer.append(value)
  }

  /**
   * Clear the buffer after a successful flush to disk.
   */
  def clearDelta(): Unit = {
    deltaBuffer.clear()
  }

  /**
   * Helper to get current delta as an array (for flushing).
   */
  def toArray: Array[Int] = deltaBuffer.toArray
}