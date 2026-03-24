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

package org.awandb

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.awandb.core.jni.NativeBridge
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class BufferPoolSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val BLOCK_SIZE = 4096L

  override def beforeEach(): Unit = {
    NativeBridge.initTestBufferPool(10 * BLOCK_SIZE)
  }

  override def afterEach(): Unit = {
    NativeBridge.destroyTestBufferPool()
  }

  "BufferPool" should "honor pin counts and evict the Least Recently Used unpinned page" in {
    // 1. Fill the pool to 90% capacity (9 blocks) to stay just below the 95% Hard Watermark
    for (i <- 0 until 9) {
      NativeBridge.requestTestPage(i)
    }
    NativeBridge.getBufferPoolUsagePercent() shouldBe 90

    // 2. Pin blocks 1 through 8. Leave block 0 UNPINNED.
    for (i <- 1 until 9) {
      NativeBridge.pinTestPage(i)
    }

    // 3. Request a 10th block. This simulates a query pushing memory to 100% (hitting the 95% Hard Limit!)
    NativeBridge.requestTestPage(9)

    // 4. Assertions
    NativeBridge.isPageResident(0) shouldBe false // Unpinned, evicted
    NativeBridge.isPageResident(1) shouldBe true  // Pinned, survived
    NativeBridge.isPageResident(9) shouldBe true  // Newly loaded
  }

  it should "trigger soft watermark background eviction down to idle levels" in {
    for (i <- 0 until 8) {
      NativeBridge.requestTestPage(i)
    }
    NativeBridge.getBufferPoolUsagePercent() shouldBe 80

    NativeBridge.triggerPacemakerSweep()

    NativeBridge.getBufferPoolUsagePercent() should be <= 20
  }

  // =========================================================================
  // THE CONCURRENCY STRESS TEST
  // =========================================================================
  it should "handle heavy concurrent access and thrashing without deadlocks or segfaults" in {
    val numThreads = 50
    val opsPerThread = 1000

    println(s"\n[TEST] Spawning $numThreads parallel threads for Buffer Pool stress test...")

    val futures = (1 to numThreads).map { threadId =>
      Future {
        val rand = new Random()
        
        for (_ <- 1 to opsPerThread) {
          // Request random pages across a space TWICE the size of the buffer pool.
          // This guarantees massive cache misses, forcing the C++ LRU eviction 
          // logic to fire constantly under heavy lock contention.
          val pageId = rand.nextInt(20) 

          // 1. Thread requests page (Locks mutex, potentially triggers eviction)
          NativeBridge.requestTestPage(pageId)
          
          // 2. Thread pins page (Simulates a running query reading the memory)
          NativeBridge.pinTestPage(pageId)

          // Simulate micro-work
          Thread.sleep(0, rand.nextInt(500)) 

          // 3. Thread unpins page (Query finished, safe to evict again)
          NativeBridge.unpinTestPage(pageId)
        }
      }
    }

    // Block and wait for all 50 threads to complete their 1,000 operations
    // If the C++ code deadlocks, this will time out after 30 seconds and fail.
    Await.result(Future.sequence(futures), 30.seconds)

    // If we reach this line, the JVM did not crash (No Segfaults!)
    // Let's verify the internal state didn't corrupt its usage counter.
    val finalUsage = NativeBridge.getBufferPoolUsagePercent()
    
    println(s"[TEST] Stress test completed successfully. Final Pool Usage: $finalUsage%")
    
    finalUsage should be <= 100
    finalUsage should be >= 0
  }

  // =========================================================================
  // EDGE CASES, NULLS, AND POINTER LIFECYCLES
  // =========================================================================

  it should "safely handle JNI calls after the pool is destroyed (Null Pointer Safety)" in {
    // 1. Nuke the pool early (simulating an abrupt Engine shutdown)
    NativeBridge.destroyTestBufferPool() 

    // 2. Attack the JNI boundary. If the C++ layer lacks null checks, 
    // any of these will instantly Segfault the JVM.
    noException should be thrownBy {
      NativeBridge.requestTestPage(99)
      NativeBridge.pinTestPage(99)
      NativeBridge.unpinTestPage(99)
      NativeBridge.triggerPacemakerSweep()
    }

    // 3. Verify safe default returns
    NativeBridge.isPageResident(99) shouldBe false
    NativeBridge.getBufferPoolUsagePercent() shouldBe 0
  }

  it should "prevent negative pin counts and ignore ghost pointers safely" in {
    NativeBridge.requestTestPage(1)
    
    // 1. Attack: Try to unpin a page multiple times (Integer Underflow attack)
    NativeBridge.unpinTestPage(1)
    NativeBridge.unpinTestPage(1)
    NativeBridge.unpinTestPage(1)

    // 2. Request new pages to force eviction. 
    // If Page 1's pin count went to -2, the eviction logic might break.
    for (i <- 2 to 12) NativeBridge.requestTestPage(i)

    // Verify Page 1 was cleanly evicted
    NativeBridge.isPageResident(1) shouldBe false

    // 3. Attack: Try to interact with "Ghost" pages 
    // (Page 1 is evicted, Page 999 never existed)
    noException should be thrownBy {
      NativeBridge.pinTestPage(999)
      NativeBridge.unpinTestPage(999)
      NativeBridge.pinTestPage(1) 
    }
  }

  it should "not leak metadata (Ghost Objects) during massive sequential scans" in {
    // 1. Simulate a full table scan over 100,000 unique pages.
    // The physical memory (4KB blocks) is capped by the LRU, but if the C++ 
    // map isn't deleting the metadata for evicted pages, RAM will bloat endlessly!
    
    for (i <- 1 to 100000) {
      NativeBridge.requestTestPage(i)
    }

    // 2. The usage should still strictly obey the 100% cap
    val finalUsage = NativeBridge.getBufferPoolUsagePercent()
    finalUsage should be <= 100

    // 3. Only the very last few pages should actually be resident
    NativeBridge.isPageResident(100000) shouldBe true
    NativeBridge.isPageResident(1) shouldBe false // Long gone
  }
}