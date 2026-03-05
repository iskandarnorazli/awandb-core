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

  // Memory waiting to be freed. Tuple of (MemoryPointer, RetiredEpoch, isBlock)
  // [FIX] Update tuple to include an `isBlock: Boolean` flag
  private val retirementList = new ConcurrentLinkedQueue[(Long, Long, Boolean)]()

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
   * [FIX] Add isBlock parameter with a default of false to safely differentiate object deletion
   */
  def retire(ptr: Long, isBlock: Boolean = false): Unit = {
    retirementList.offer((ptr, globalEpoch.get(), isBlock))
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
      // [BUG FIX] Handle the race condition where localEpochs empties during the .min evaluation
      val minActiveEpoch = scala.util.Try(localEpochs.values().asScala.min).getOrElse(globalEpoch.get())

      // We can only free memory that was retired strictly BEFORE the oldest active epoch.
      val it = retirementList.iterator()
      while (it.hasNext) {
        val (ptr, retiredEpoch, isBlock) = it.next() // [NEW] Extract isBlock
        if (retiredEpoch < minActiveEpoch) {
          if (isBlock) releaser.freeBlock(ptr) else releaser.free(ptr) // [NEW] Route correctly
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