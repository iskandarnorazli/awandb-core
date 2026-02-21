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

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import java.io.File

class FusedJoinSpec extends AnyFunSuite {

  NativeBridge.init()

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  // =================================================================================
  // TEST 1: The Standard Pipeline
  // =================================================================================
  test("Star Schema God Kernel: Should pipelined probe and aggregate without intermediate arrays") {
    
    val uniqueTestDir = s"./data/test_star_schema_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs()

    // 1. SETUP DIMENSION TABLE (Build Side)
    val dimTable = new AwanTable("categories", 1000, uniqueTestDir)
    dimTable.addColumn("id")            // Col 0: Join Key
    dimTable.addColumn("category_code") // Col 1: Group By Attribute (Payload)
    
    dimTable.insertRow(Array(1, 100))
    dimTable.insertRow(Array(2, 200))
    dimTable.flush()

    // 2. SETUP FACT TABLE (Probe Side)
    val factTable = new AwanTable("sales", 1000, uniqueTestDir)
    factTable.addColumn("id")      // Col 0: Fact ID
    factTable.addColumn("dim_id")  // Col 1: Foreign Key
    factTable.addColumn("price")   // Col 2: Measure to SUM

    factTable.insertRow(Array(101, 1, 50))  
    factTable.insertRow(Array(102, 1, 150)) 
    factTable.insertRow(Array(103, 2, 300)) 
    factTable.insertRow(Array(104, 3, 999)) // Orphan fact
    factTable.flush()

    // 3. EXECUTE
    val resultMap = factTable.executeStarQuery(dimTable, "id", "category_code", "dim_id", "price")

    // 4. ASSERTIONS
    assert(resultMap.contains(100), "Missing category 100")
    assert(resultMap(100) == 200, s"Expected 200 for Electronics, got ${resultMap(100)}")
    assert(resultMap.contains(200), "Missing category 200")
    assert(resultMap(200) == 300, s"Expected 300 for Clothing, got ${resultMap(200)}")
    assert(!resultMap.contains(3), "Inner join should drop unmatched probe keys")

    dimTable.close()
    factTable.close()
    deleteRecursively(testDirFile)
  }

  // =================================================================================
  // TEST 2: Multi-Block Streaming
  // =================================================================================
  test("Star Schema God Kernel: Should handle multi-block fact tables seamlessly") {
    
    val uniqueTestDir = s"./data/test_star_schema_multiblock_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs()

    val dimTable = new AwanTable("categories_mb", 1000, uniqueTestDir)
    dimTable.addColumn("id")
    dimTable.addColumn("category_code")
    dimTable.insertRow(Array(1, 10)) // Group 10
    dimTable.insertRow(Array(2, 20)) // Group 20
    dimTable.flush()

    val factTable = new AwanTable("sales_mb", 1000, uniqueTestDir)
    factTable.addColumn("id")
    factTable.addColumn("dim_id")
    factTable.addColumn("price")

    // Force creation of Block 1 natively
    for (i <- 1 to 5) factTable.insertRow(Array(i, 1, 100))
    factTable.flush() 
    
    // Force creation of Block 2 natively
    for (i <- 6 to 10) factTable.insertRow(Array(i, 2, 200))
    factTable.flush() 
    
    // Force creation of Block 3 natively
    for (i <- 11 to 15) factTable.insertRow(Array(i, 1, 100))
    factTable.flush()

    // Execute query across all 3 blocks
    val resultMap = factTable.executeStarQuery(dimTable, "id", "category_code", "dim_id", "price")

    // Assertions
    assert(resultMap.size == 2, "Should only output two distinct groups")
    
    // Group 10 (id 1) -> 10 rows total across Block 1 and Block 3 * 100 = 1000
    assert(resultMap(10) == 1000, s"Expected Group 10 to be 1000, got ${resultMap.getOrElse(10, 0)}")
    
    // Group 20 (id 2) -> 5 rows total in Block 2 * 200 = 1000
    assert(resultMap(20) == 1000, s"Expected Group 20 to be 1000, got ${resultMap.getOrElse(20, 0)}")

    dimTable.close()
    factTable.close()
    deleteRecursively(testDirFile)
  }

  // =================================================================================
  // TEST 3: Null Pointers / Empty Table Graceful Handling
  // =================================================================================
  test("Star Schema God Kernel: Should gracefully handle completely empty tables") {
    
    val uniqueTestDir = s"./data/test_star_schema_empty_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs()

    val dimTable = new AwanTable("categories_empty", 1000, uniqueTestDir)
    dimTable.addColumn("id")
    dimTable.addColumn("category_code")
    
    val factTable = new AwanTable("sales_empty", 1000, uniqueTestDir)
    factTable.addColumn("id")
    factTable.addColumn("dim_id")
    factTable.addColumn("price")

    // SCENARIO A: Both tables are completely empty
    val resA = factTable.executeStarQuery(dimTable, "id", "category_code", "dim_id", "price")
    assert(resA.isEmpty, "Result should be empty when both tables are empty")

    // SCENARIO B: Fact has data, but Dimension map is empty
    factTable.insertRow(Array(1, 1, 500))
    factTable.flush()
    val resB = factTable.executeStarQuery(dimTable, "id", "category_code", "dim_id", "price")
    assert(resB.isEmpty, "Result should be empty when dim table is empty")

    dimTable.close()
    factTable.close()
    deleteRecursively(testDirFile)
  }

  // =================================================================================
  // TEST 4: Zero Match / Complete Miss Handling
  // =================================================================================
  test("Star Schema God Kernel: Should return empty map if no foreign keys match") {
    
    val uniqueTestDir = s"./data/test_star_schema_miss_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs()

    val dimTable = new AwanTable("categories_miss", 1000, uniqueTestDir)
    dimTable.addColumn("id")
    dimTable.addColumn("category_code")
    dimTable.insertRow(Array(99, 999)) // ID is 99
    dimTable.flush()
    
    val factTable = new AwanTable("sales_miss", 1000, uniqueTestDir)
    factTable.addColumn("id")
    factTable.addColumn("dim_id")
    factTable.addColumn("price")
    factTable.insertRow(Array(1, 1, 500)) // Fact looks for ID 1
    factTable.flush()

    // Execute
    val resultMap = factTable.executeStarQuery(dimTable, "id", "category_code", "dim_id", "price")

    // Assert
    assert(resultMap.isEmpty, "Result should be completely empty since no probe keys matched the build map")

    dimTable.close()
    factTable.close()
    deleteRecursively(testDirFile)
  }
}