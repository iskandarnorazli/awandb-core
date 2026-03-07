/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.engine.AwanTable
import java.nio.file.Files

class CuckooFilterIntegrationSpec extends AnyFunSuite {

  test("BlockManager should build and query Cuckoo Filter successfully") {
    // 1. Setup an isolated test table
    val tempDir = Files.createTempDirectory("awandb_test").toString
    val table = new AwanTable("test_cuckoo", 1000, dataDir = tempDir)
    
    table.addColumn("id", isString = false, useDictionary = false, isVector = false)

    try {
      // 2. Insert test data and flush to disk (creates Block 0)
      table.insertRow(Array(100))
      table.insertRow(Array(200))
      table.flush()
      
      // Ensure the block was actually created
      assert(table.blockManager.getLoadedBlocks.size == 1, "Block was not flushed to disk")
      
      // 3. Manually trigger the index builder (simulating the background daemon)
      // [FIX] Pass the vector flag for our single column
      val builtCount = table.blockManager.buildPendingIndexes(Array(false))
      assert(builtCount == 1, "Expected 1 Cuckoo Filter to be built")
      
      // 4. Query the Filter directly
      // [FIX] Pass the colIdx (0) before the key
      assert(table.blockManager.mightContain(0, 0, 100) === true, "Filter missed existing key 100")
      assert(table.blockManager.mightContain(0, 0, 200) === true, "Filter missed existing key 200")
      
      // 999 was never inserted, should return false
      assert(table.blockManager.mightContain(0, 0, 999) === false, "Filter returned false positive for 999")
      
    } finally {
      table.close()
    }
  }

  test("Daemon should automatically build Cuckoo Filters in the background") {
    val tempDir = java.nio.file.Files.createTempDirectory("awandb_test_daemon").toString
    
    // We set daemonIntervalMs to 100ms so the test runs fast
    val table = new AwanTable("test_daemon", 1000, dataDir = tempDir, daemonIntervalMs = 100L)
    table.addColumn("id", isString = false, useDictionary = false, isVector = false)

    try {
      table.insertRow(Array(500))
      table.flush() // Flushes to disk. Block is now in pendingIndexes.

      // Wait for the daemon to wake up, see the pending index, and build it
      Thread.sleep(500) 

      // If the daemon is doing its job, the filter array will be built and the first col pointer will NOT be 0
      // [FIX] Use getFilterArray and grab the first column's pointer
      val filterArray = table.blockManager.getFilterArray(0)
      assert(filterArray != null && filterArray(0) != 0L, "Daemon failed to build the Cuckoo Filter automatically!")
      
    } finally {
      table.close()
    }
  }
}