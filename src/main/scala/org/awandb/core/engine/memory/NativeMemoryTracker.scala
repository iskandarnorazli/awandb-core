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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

object NativeMemoryTracker {
  // Tracks total bytes currently held in C++ RAM
  private val totalAllocatedBytes = new AtomicLong(0L)
  
  // Ledger of specific active pointers using Java's boxed Long to safely handle nulls
  private val activeAllocations = new ConcurrentHashMap[java.lang.Long, java.lang.Long]()

  def recordAllocation(ptr: Long, sizeBytes: Long): Unit = {
    if (ptr != 0L) {
      // [CRITICAL FIX] Use putIfAbsent to prevent double-ledgering
      // It returns null if the key was not already associated with a value
      val previousValue = activeAllocations.putIfAbsent(ptr, sizeBytes)
      
      // Only increment the global byte tracker if this is a brand new pointer
      if (previousValue == null) {
        totalAllocatedBytes.addAndGet(sizeBytes)
      } else {
        // Warning: The engine attempted to track an already tracked pointer.
        // By ignoring it here, we protect the totalAllocatedBytes ledger from drifting.
      }
    }
  }

  def recordDeallocation(ptr: Long): Unit = {
    if (ptr != 0L) {
      // Safely returns null if the pointer wasn't in the map
      val size = activeAllocations.remove(ptr)
      if (size != null) {
        totalAllocatedBytes.addAndGet(-size)
      }
    }
  }

  def getActiveBytes: Long = totalAllocatedBytes.get()
  def getActivePointerCount: Int = activeAllocations.size()

  /**
   * TDD Hook: Call this at the end of every ScalaTest.
   * If it throws, you immediately know the exact test that caused a memory leak!
   */
  def assertNoLeaks(): Unit = {
    val bytes = getActiveBytes
    val count = getActivePointerCount
    if (bytes > 0 || count > 0) {
      val leakedPointers = activeAllocations.keys().asScala.take(5).map(p => f"0x$p%016x").mkString(", ")
      throw new AssertionError(
        s"NATIVE MEMORY LEAK DETECTED! $bytes bytes leaked across $count active pointers.\n" +
        s"Sample leaked pointers: [$leakedPointers]"
      )
    }
  }

  def reset(): Unit = {
    activeAllocations.clear()
    totalAllocatedBytes.set(0L)
  }
}