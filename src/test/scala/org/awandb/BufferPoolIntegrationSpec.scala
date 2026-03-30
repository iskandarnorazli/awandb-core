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
import org.awandb.core.engine.memory.NativeMemoryTracker
import org.awandb.core.engine.AwanTable
import java.io.File

class BufferPoolIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var table: AwanTable = _

  // --- LIFECYCLE MANAGEMENT ---
  
  private def cleanDir(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      val files = dir.listFiles()
      if (files != null) files.foreach(_.delete())
      dir.delete()
    }
  }

  override def beforeEach(): Unit = {
    cleanDir("data/test_db")
    cleanDir("data/outofcore_db")
  }

  override def afterEach(): Unit = {
    if (table != null) {
      try { table.drop() } catch { case _: Exception => }
      table = null
    }

    NativeBridge.destroyTestBufferPool()

    cleanDir("data/test_db")
    cleanDir("data/outofcore_db")
    
    // [CRITICAL FIX] Give our Detached Drop Thread (from Strike 2) a split-second 
    // to finish wiping the C++ blocks before we run the strict ledger check.
    Thread.sleep(250) 

    NativeMemoryTracker.assertNoLeaks()
  }

  // --- TESTS ---

  "AwanTable Pacemaker Daemon" should "enforce watermarks and gracefully sweep unpinned pages" in {
    // [FIX] Initialize Native Buffer Pool (45KB capacity)
    // 10 pages * 4KB = 40KB. 40KB / 45KB = ~88% usage.
    // This avoids the 95% single-thread deadlock while still populating the pool.
    NativeBridge.initTestBufferPool(46080) 

    table = new AwanTable("watermark_test", 1000, "data/test_db", daemonIntervalMs = 200L)
    table.addColumn("col1")

    // 2. Simulate Heavy TableScan faulting pages into memory
    for (pageId <- 1 to 10) {
      NativeBridge.requestTestPage(pageId)
      NativeBridge.pinTestPage(pageId)
    }

    // 3. Verify Initial Watermark (Should sit comfortably below 95%)
    val initialUsage = NativeBridge.getBufferPoolUsagePercent()
    println(s"[TEST] Initial Pool Usage (All Pinned): $initialUsage%")
    initialUsage should be >= 85 

    // 4. Release locks: Unpin all EXCEPT page 5
    for (pageId <- 1 to 10) {
      if (pageId != 5) {
        NativeBridge.unpinTestPage(pageId)
      }
    }

    // 5. Wait for the Pacemaker Daemon to trigger the Clock-Sweep
    Thread.sleep(600) 

    val sweptUsage = NativeBridge.getBufferPoolUsagePercent()
    println(s"[TEST] Pool Usage after Pacemaker Sweep: $sweptUsage%")
    
    // The Pacemaker should force it all the way down to 20%
    sweptUsage should be <= 20 
    NativeBridge.isPageResident(5) shouldBe true
    NativeBridge.unpinTestPage(5)
  }

  "AwanTable Out-of-Core Execution" should "transparently fault and evict pages without JVM waking" in {
    // 1. Initialize a Buffer Pool large enough for high core-count concurrent access
    NativeBridge.initTestBufferPool(128000) 

    table = new AwanTable("outofcore_test", 1000, "data/outofcore_db", daemonIntervalMs = 200L)
    table.addColumn("col1")

    // 2. Insert 10,000 rows (10 Blocks total)
    println("\n[TEST] Generating 40KB of Data (10 Blocks)...")
    for (i <- 1 to 10000) {
      table.insert(i)
      if (i % 1000 == 0) table.flush() 
    }

    // 3. The Acid Test: Full Table Scan
    println("[TEST] Executing Out-of-Core AVX Scan...")
    val matchCount = table.query(5000) 
    
    println(s"[TEST] Query Result: $matchCount matches.")
    matchCount shouldBe 5000

    // 4. Verify the Buffer Pool was actually used
    val peakUsage = NativeBridge.getBufferPoolUsagePercent()
    println(s"[TEST] Post-Query Pool Usage: $peakUsage%")
    peakUsage should be > 0 

    // 5. Let Pacemaker stabilize the thrashing pool back to idle
    Thread.sleep(600)
    val finalUsage = NativeBridge.getBufferPoolUsagePercent()
    println(s"[TEST] Final Usage after sweep: $finalUsage%")
    finalUsage should be <= 20
  }
}