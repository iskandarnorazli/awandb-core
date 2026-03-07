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

package org.awandb.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.io.File

class AwanTableScanSpec extends AnyFunSuite with BeforeAndAfterAll {

  val testDir = "data/test_scan"

  override def afterAll(): Unit = {
    // Cleanup disk after tests
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    dir.delete()
  }

  test("scanFiltered should eagerly materialize disk records to prevent EBMM leaks") {
    // 1. Setup Table
    val table = new AwanTable("test_users", 1000, dataDir = testDir)
    table.addColumn("id", isString = false)
    table.addColumn("age", isString = false)

    // 2. Insert Data
    table.insertBatch("id", Array(1, 2, 3, 4, 5))
    table.insertBatch("age", Array(20, 30, 30, 40, 50))
    
    // 3. Flush to Disk (Forces data into native C++ blocks)
    table.flush()

    // 4. Perform the Filtered Scan
    // OpType 0 = Equality. We are looking for age == 30.
    // The withEpoch block is acquired and released entirely inside this method call.
    val resultIter = table.scanFiltered("age", 0, 30)

    // 5. Consume the Iterator safely OUTSIDE the epoch boundary
    // If the iterator was lazy, this would segfault or read garbage memory here.
    val results = resultIter.toList

    // 6. Assertions
    assert(results.length == 2, "Should have found exactly 2 users with age 30")
    
    val matchedIds = results.map(row => row(0).asInstanceOf[Int]).toSet
    assert(matchedIds.contains(2), "User ID 2 should be in results")
    assert(matchedIds.contains(3), "User ID 3 should be in results")

    table.close()
  }
}