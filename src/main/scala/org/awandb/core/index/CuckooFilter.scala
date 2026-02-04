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

class CuckooFilter(val capacity: Int, existingPointer: Long = 0) extends AutoCloseable {
  
  // If an existing pointer is provided (e.g., loaded from disk), use it.
  // Otherwise, create a new one.
  private var pointer: Long = if (existingPointer != 0) existingPointer else NativeBridge.cuckooCreate(capacity)
  
  if (pointer == 0) throw new OutOfMemoryError("Failed to allocate Cuckoo Filter")

  /**
   * Insert a single key. Returns true if successful, false if full (and kicking failed).
   */
  def insert(key: Int): Boolean = {
    if (pointer == 0) throw new IllegalStateException("Filter closed")
    NativeBridge.cuckooInsert(pointer, key)
  }

  /**
   * Batch insertion (High Performance).
   * Uses software prefetching in C++ to hide RAM latency.
   */
  def insertBatch(data: Array[Int]): Unit = {
    if (pointer == 0) throw new IllegalStateException("Filter closed")
    NativeBridge.cuckooBuildBatch(pointer, data)
  }

  /**
   * Check if a key exists. 
   * False Positive Rate: ~3% (with current bucket size 4 / fingerprint 16-bit).
   */
  def contains(key: Int): Boolean = {
    if (pointer == 0) return false
    NativeBridge.cuckooContains(pointer, key)
  }
  
  /**
   * Persist the filter state to disk.
   */
  def save(path: String): Boolean = {
    if (pointer == 0) throw new IllegalStateException("Filter closed")
    NativeBridge.cuckooSave(pointer, path)
  }

  override def close(): Unit = {
    if (pointer != 0) {
      NativeBridge.cuckooDestroy(pointer)
      pointer = 0
    }
  }
}

/**
 * Factory for loading Filters from disk.
 */
object CuckooFilter {
  def load(path: String): CuckooFilter = {
    val ptr = NativeBridge.cuckooLoad(path)
    if (ptr == 0) throw new RuntimeException(s"Failed to load CuckooFilter from $path")
    
    // Capacity is embedded in the file, so we pass 0 as a dummy capacity
    // The C++ side handles the reconstruction.
    new CuckooFilter(0, ptr)
  }
}