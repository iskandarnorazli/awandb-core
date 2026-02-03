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

package org.awandb.core.index

import org.awandb.core.jni.NativeBridge

class CuckooFilter(val capacity: Int) extends AutoCloseable {
  
  private var pointer: Long = NativeBridge.cuckooCreate(capacity)
  
  if (pointer == 0) throw new OutOfMemoryError("Failed to allocate Cuckoo Filter")

  def insert(key: Int): Boolean = {
    if (pointer == 0) throw new IllegalStateException("Filter closed")
    NativeBridge.cuckooInsert(pointer, key)
  }

  def contains(key: Int): Boolean = {
    if (pointer == 0) return false
    NativeBridge.cuckooContains(pointer, key)
  }
  
  def load(data: Array[Int]): Unit = {
    if (pointer == 0) throw new IllegalStateException("Filter closed")
    NativeBridge.cuckooBuildBatch(pointer, data)
  }

  override def close(): Unit = {
    if (pointer != 0) {
      NativeBridge.cuckooDestroy(pointer)
      pointer = 0
    }
  }
}