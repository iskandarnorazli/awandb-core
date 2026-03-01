/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.engine.memory

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.ArrayBuffer

class EpochManagerSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  class MockMemoryReleaser extends MemoryReleaser {
    val freedPointers = ArrayBuffer[Long]()
    override def free(ptr: Long): Unit = synchronized {
      freedPointers.append(ptr)
    }
  }

  var releaser: MockMemoryReleaser = _
  var epochManager: EpochManager = _

  override def beforeEach(): Unit = {
    // Ensure total isolation between tests.
    // This prevents Epoch 10 from Test A leaking into Test B.
    releaser = new MockMemoryReleaser()
    epochManager = new EpochManager(releaser)
  }

  override def afterEach(): Unit = {
    // Explicit teardown
    releaser.freedPointers.clear()
    releaser = null
    epochManager = null
  }

  test("EpochManager should not free memory if a thread is pinning the epoch") {
    val threadId = 1L
    epochManager.registerThread(threadId) // Thread pins at Epoch 0

    epochManager.retire(100L) // Block retired at Epoch 0

    epochManager.tryReclaim()

    // Assert memory is NOT freed
    releaser.freedPointers should not contain (100L)
    epochManager.getPendingReclaims shouldBe 1
  }

  test("EpochManager should free memory after all threads advance past the retirement epoch") {
    val threadId = 1L
    epochManager.registerThread(threadId) // Thread pins at Epoch 0

    epochManager.retire(200L) // Retired at Epoch 0

    epochManager.advanceGlobalEpoch() // Global is now 1

    // Reclaim should fail because Thread is still pinned at Epoch 0
    epochManager.tryReclaim()
    releaser.freedPointers should not contain (200L)

    // Thread finishes work and grabs the new epoch
    epochManager.updateLocalEpoch(threadId) // Thread pins at Epoch 1

    // Now reclaim should succeed (Retired 0 < MinActive 1)
    epochManager.tryReclaim()
    releaser.freedPointers should contain (200L)
    epochManager.getPendingReclaims shouldBe 0
  }

  test("EpochManager should handle thread deregistration correctly") {
    val threadId = 2L
    epochManager.registerThread(threadId)

    epochManager.retire(300L)
    epochManager.advanceGlobalEpoch() // Global = 1

    epochManager.tryReclaim()
    releaser.freedPointers should not contain (300L)

    // Thread finishes and completely disconnects
    epochManager.deregisterThread(threadId)

    epochManager.tryReclaim()
    releaser.freedPointers should contain (300L)
  }
  
  test("EpochManager should free memory immediately if no threads are active") {
    // No threads registered
    epochManager.retire(400L) // Retired at Epoch 0
    
    // Global epoch moves to 1
    epochManager.advanceGlobalEpoch()
    
    epochManager.tryReclaim()
    
    // Since no threads are active, the min active epoch defaults to Global (1).
    // Retired (0) < Global (1), so it frees immediately.
    releaser.freedPointers should contain (400L)
  }

  test("EpochManager should respect the slowest active thread when reclaiming") {
    // Thread 1 pins at Epoch 0
    epochManager.registerThread(1L) 
    epochManager.retire(100L) // Retired at Epoch 0

    // Global moves to 1
    epochManager.advanceGlobalEpoch() 
    
    // Thread 2 pins at Epoch 1
    epochManager.registerThread(2L) 
    epochManager.retire(200L) // Retired at Epoch 1

    // Global moves to 2
    epochManager.advanceGlobalEpoch() 
    
    // Thread 1 finishes its work and updates to Epoch 2. 
    // BUT Thread 2 is still lagging behind at Epoch 1!
    epochManager.updateLocalEpoch(1L) 

    epochManager.tryReclaim()
    
    // minActiveEpoch should be 1 (because Thread 2 is holding it back).
    // Memory retired at 0 (100L) is strictly less than 1, so it CAN be freed.
    // Memory retired at 1 (200L) is NOT strictly less than 1, so it MUST be kept.
    releaser.freedPointers should contain (100L)
    releaser.freedPointers should not contain (200L)
    epochManager.getPendingReclaims shouldBe 1
  }

  test("EpochManager should safely handle empty retirement lists") {
    epochManager.advanceGlobalEpoch()
    // Should not throw any exceptions when nothing is there to reclaim
    noException should be thrownBy epochManager.tryReclaim()
    releaser.freedPointers shouldBe empty
  }

  test("EpochManager should update thread epoch if re-registered instead of duplicating") {
    epochManager.registerThread(1L) // Pins at 0
    epochManager.advanceGlobalEpoch() // Global is 1
    
    // Thread re-registers (e.g., started a new query)
    epochManager.registerThread(1L) // Should now pin at 1
    
    epochManager.retire(500L) // Retired at 1
    epochManager.advanceGlobalEpoch() // Global is 2
    
    epochManager.tryReclaim()
    // Since thread 1 is pinned at 1, memory retired at 1 cannot be freed yet
    releaser.freedPointers should not contain (500L)
    
    epochManager.updateLocalEpoch(1L) // Advances to 2
    epochManager.tryReclaim()
    releaser.freedPointers should contain (500L)
  }

  test("EpochManager should handle massive retirement volumes efficiently") {
    val threadId = 1L
    epochManager.registerThread(threadId)
    
    val volume = 100000
    for (i <- 1 to volume) {
      epochManager.retire(i.toLong)
    }
    
    epochManager.getPendingReclaims shouldBe volume
    
    epochManager.advanceGlobalEpoch()
    epochManager.updateLocalEpoch(threadId)
    epochManager.tryReclaim()
    
    releaser.freedPointers.size shouldBe volume
    epochManager.getPendingReclaims shouldBe 0
  }

  test("EpochManager should remain thread-safe under concurrent stress") {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.{Future, Await}
    import scala.concurrent.duration._

    val threadCount = 200
    
    // Simulate 200 concurrent worker threads registering, advancing epochs, 
    // retiring memory, and attempting reclaims simultaneously.
    val futures = (1 to threadCount).map { i =>
      Future {
        val tId = i.toLong
        epochManager.registerThread(tId)
        
        // Randomly advance the global epoch to create chaos
        if (i % 10 == 0) epochManager.advanceGlobalEpoch()
        
        epochManager.retire(tId * 1000) // Unique pointer per thread
        
        epochManager.updateLocalEpoch(tId)
        epochManager.tryReclaim()
        epochManager.deregisterThread(tId)
      }
    }
    
    Await.result(Future.sequence(futures), 10.seconds)
    
    // Final cleanup: push global epoch forward and reclaim anything leftover
    epochManager.advanceGlobalEpoch()
    epochManager.tryReclaim()
    
    // Every single pointer should have been safely captured and freed
    releaser.freedPointers.size shouldBe threadCount
    epochManager.getPendingReclaims shouldBe 0
  }
}