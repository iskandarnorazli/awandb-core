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

package org.awandb.core.engine.memory

trait MemoryReleaser {
  /**
   * Frees native memory based on the resource type.
   * @param ptr The native memory pointer.
   * @param resourceType 0 = Raw Memory, 1 = Data Block, 2 = Cuckoo Filter
   */
  def free(ptr: Long, resourceType: Int): Unit
}

class NativeMemoryReleaser extends MemoryReleaser {
  override def free(ptr: Long, resourceType: Int): Unit = {
    resourceType match {
      case 0 => 
        // Free raw memory buffers (e.g., bitmasks, temporary query buffers)
        org.awandb.core.jni.NativeBridge.freeMainStore(ptr)
      case 1 => 
        // Invoke the C++ destructor for AwanDB Blocks
        org.awandb.core.jni.NativeBridge.destroyBlock(ptr)
      case 2 => 
        // Invoke the C++ destructor for Cuckoo Filters
        org.awandb.core.jni.NativeBridge.cuckooDestroy(ptr)
      case _ => 
        throw new IllegalArgumentException(s"Unknown resource type for memory reclamation: $resourceType")
    }
  }
}