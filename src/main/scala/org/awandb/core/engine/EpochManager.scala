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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.jdk.CollectionConverters._

class EpochManager(releaser: MemoryReleaser) {

  // The master clock. Incremented periodically (e.g., every 10ms or after compactions).
  private val globalEpoch = new AtomicLong(0L)

  // Tracks which epoch each thread is currently operating in.
  private val localEpochs = new ConcurrentHashMap[Long, Long]()

  // Memory waiting to be freed. Tuple of (MemoryPointer, RetiredEpoch)
  private val retirementList = new ConcurrentLinkedQueue[(Long, Long)]()

  // [NEW] Non-blocking lock for the garbage collector
  private val isReclaiming = new AtomicBoolean(false)

  /**
   * Called by a thread (e.g., MorselExec worker) before it starts scanning.
   * It "pins" the thread to the current global epoch.
   */
  def registerThread(threadId: Long): Unit = {
    localEpochs.put(threadId, globalEpoch.get())
  }

  /**
   * Called by a thread when it finishes its unit of work.
   */
  def deregisterThread(threadId: Long): Unit = {
    localEpochs.remove(threadId)
  }

  /**
   * Refreshes a thread's pinned epoch to the latest global epoch.
   */
  def updateLocalEpoch(threadId: Long): Unit = {
    if (localEpochs.containsKey(threadId)) {
      localEpochs.put(threadId, globalEpoch.get())
    }
  }

  /**
   * Moves the global clock forward.
   */
  def advanceGlobalEpoch(): Unit = {
    globalEpoch.incrementAndGet()
  }

  /**
   * Marks a native pointer for deletion. It is NOT freed yet.
   */
  def retire(ptr: Long): Unit = {
    retirementList.offer((ptr, globalEpoch.get()))
  }

  /**
   * Evaluates the retirement list and frees memory if no threads can access it.
   */
  def tryReclaim(): Unit = {
    if (retirementList.isEmpty) return

    // [FIX] Try-Lock: Only one thread can perform reclamation at a time.
    // If another thread is already reclaiming, return immediately (Lock-Free).
    if (!isReclaiming.compareAndSet(false, true)) return

    try {
      // Find the oldest epoch currently being viewed by any active thread.
      // If no threads are active, the safe epoch is the current global epoch.
      val minActiveEpoch = if (localEpochs.isEmpty) {
        globalEpoch.get()
      } else {
        localEpochs.values().asScala.min
      }

      // We can only free memory that was retired strictly BEFORE the oldest active epoch.
      // If memory was retired at Epoch N, and a thread is pinned at Epoch N, it might still have a reference.
      val it = retirementList.iterator()
      while (it.hasNext) {
        val (ptr, retiredEpoch) = it.next()
        if (retiredEpoch < minActiveEpoch) {
          releaser.free(ptr)
          it.remove() // Remove from the queue
        }
      }
    } finally {
      // Release the lock
      isReclaiming.set(false)
    }
  }
  
  // Expose for testing
  def getGlobalEpoch: Long = globalEpoch.get()
  def getPendingReclaims: Int = retirementList.size()
}