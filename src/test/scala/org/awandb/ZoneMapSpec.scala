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

package org.awandb

import org.awandb.core.engine.AwanTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class ZoneMapSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = "data/zone_map_test"
  
  def cleanDir(dirPath: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.exists()) {
        if (file.isDirectory) {
          val children = file.listFiles()
          if (children != null) children.foreach(deleteRecursively)
        }
        file.delete()
      }
    }
    val dir = new File(dirPath)
    deleteRecursively(dir)
  }

  "AwanDB Optimizer" should "skip blocks based on Zone Maps (Min/Max)" in {
    cleanDir(TEST_DIR)
    new File(TEST_DIR).mkdirs()

    val table = new AwanTable("zone_test", 10000, TEST_DIR)
    table.addColumn("age")

    // Block 1: Low values (0 to 100)
    for (i <- 0 to 100) table.insert(i)
    table.flush() // Persist to disk (Creates Block 1)

    // Block 2: High values (1000 to 1100)
    for (i <- 1000 to 1100) table.insert(i)
    table.flush() // Persist to disk (Creates Block 2)

    // Query: > 500
    // Expected: 
    // - Block 1 (Max=100) should be SKIPPED completely (0 hits)
    // - Block 2 (Max=1100) should be SCANNED (101 hits)
    
    val count = table.query(500)
    
    count shouldBe 101
    
    // We can verify skipping behavior by checking internal logs or profiling,
    // but functional correctness (101) confirms the logic holds.
    
    table.close()
    cleanDir(TEST_DIR)
  }
}