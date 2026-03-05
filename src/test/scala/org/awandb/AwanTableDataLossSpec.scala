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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge

class AwanTableDataLossSpec extends AnyFunSuite with BeforeAndAfterEach {

  val testDir = "target/data_loss_repro"

  override def beforeEach(): Unit = {
    NativeBridge.init()
    val dir = new java.io.File(testDir)
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("BUG 1: RAM Row Amnesia (Index Loss during Compaction)") {
    val table = new AwanTable("ram_amnesia", 100, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)
    
    println("[Test 1] Inserting Disk Rows...")
    table.insertRow(Array(1))
    table.insertRow(Array(2))
    table.flush() // Rows 1 & 2 are now on Disk

    println("[Test 1] Inserting RAM Rows...")
    table.insertRow(Array(3))
    table.insertRow(Array(4))
    // Rows 3 & 4 are in RAM (deltaIntBuffer). They are mapped in primaryIndex with blockIdx = -1.

    println("[Test 1] Compacting Disk Rows...")
    table.delete(2)
    table.compactor.compact(0.0) // This clears and rebuilds the Primary Index using ONLY disk blocks!

    println("[Test 1] Attempting to read RAM Row 3...")
    val row = table.getRow(3)
    
    // EXPECTED FAILURE: The index rebuild completely forgot about RAM rows.
    assert(row.isDefined, "FATAL: RAM Row 3 was permanently erased from the index!")
    
    table.close()
  }

  test("BUG 2: Vector Black Hole (Embeddings zeroed out during compaction)") {
    val table = new AwanTable("vector_blackhole", 100, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)
    table.addColumn("embed", isString = false, isVector = true)

    val vec1 = Array(1.0f, 0.0f, 0.0f, 0.0f)
    val vec2 = Array(0.0f, 1.0f, 0.0f, 0.0f)

    println("[Test 2] Inserting Vectors and Flushing to Disk...")
    table.insertRow(Array(1, vec1))
    table.insertRow(Array(2, vec2))
    table.flush()

    println("[Test 2] Forcing Compaction (Vector dropping occurs here)...")
    table.delete(2)
    table.compactor.compact(0.0)

    println("[Test 2] Querying surviving vector using exact match...")
    // Since vec1 survived, querying for vec1 with a 0.9 threshold should return ID 1.
    // EXPECTED FAILURE: The C++ compaction dropped the vector, leaving it as [0.0, 0.0, 0.0, 0.0].
    // Cosine similarity against a zero-vector is mathematically undefined (or 0), so no match will be found.
    val results = table.queryVector("embed", vec1, 0.9f)
    
    assert(results.contains(1), "FATAL: Vector data was destroyed during native compaction!")
    
    table.close()
  }

  test("BUG 3: Address Reuse & Stale Map Keys (JVM SIGSEGV Crash)") {
    val table = new AwanTable("stale_pointers", 100, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)

    println("[Test 3] Allocating Block A...")
    table.insertRow(Array(1))
    table.insertRow(Array(2))
    table.flush()

    println("[Test 3] Deleting row and triggering compaction (Frees Block A)...")
    table.delete(1) // Allocates native deletion bitmask!
    table.compactor.compact(0.0)
    
    // Reclaim memory to OS immediately
    table.epochManager.advanceGlobalEpoch()
    table.epochManager.tryReclaim() 

    println("[Test 3] Allocating Block B (OS will likely reuse Block A's memory address)...")
    table.insertRow(Array(3))
    table.insertRow(Array(4))
    table.flush() // BlockManager uses the pointer address as a HashMap key!

    println("[Test 3] Deleting row in Block B (Triggers stale Bitmask lookup)...")
    table.delete(3) 

    println("[Test 3] Syncing Bitmasks to Native Memory...")
    // EXPECTED CRASH: BlockManager finds the stale pointer in `nativeBitmaps` and writes to it.
    // Writing to freed memory triggers an instant JVM Segmentation Fault!
    table.compactor.compact(0.0) 
    
    table.close()
  }
}