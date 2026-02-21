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
import org.awandb.core.sql.SQLHandler
import org.awandb.core.jni.NativeBridge
import java.io.File

class FusedGroupBySpec extends AnyFunSuite {

  NativeBridge.init()

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("Fused Operator: Should correctly aggregate across multiple blocks and all operators") {
    
    // 0. ABSOLUTE CLEAN SLATE: Use a uniquely generated data directory for this test run
    val uniqueTestDir = s"./data/test_fused_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs() // Ensure directory exists

    val tableName = "sales_ext"

    // 1. Setup Table (Pass the unique directory as the 3rd parameter!)
    val salesTable = new AwanTable(tableName, 1000, uniqueTestDir)
    salesTable.addColumn("id")       
    salesTable.addColumn("category") 
    salesTable.addColumn("price")    
    salesTable.addColumn("stock")    
    
    SQLHandler.register(tableName, salesTable)

    // ==========================================
    // 2. Insert BLOCK 1 Data
    // ==========================================
    salesTable.insertRow(Array(1, 10, 100, 50)) 
    salesTable.insertRow(Array(2, 10, 200, 10)) 
    salesTable.insertRow(Array(3, 20, 300, 5))  
    salesTable.flush() 

    // ==========================================
    // 3. Insert BLOCK 2 Data
    // ==========================================
    salesTable.insertRow(Array(4, 20, 400, 20)) 
    salesTable.insertRow(Array(5, 30, 500, 0))  
    salesTable.flush() 

    // ---------------------------------------------------------
    // TEST 1: Unfiltered Fused GROUP BY
    // Expected: Cat 10 = 300, Cat 20 = 700, Cat 30 = 500
    // ---------------------------------------------------------
    val sql1 = s"SELECT category, SUM(price) FROM $tableName GROUP BY category"
    val res1 = SQLHandler.execute(sql1)
    assert(res1.contains("10 | 300"), s"Expected 10 | 300 but got:\n$res1")
    assert(res1.contains("20 | 700"))
    assert(res1.contains("30 | 500"))

    // ---------------------------------------------------------
    // TEST 2: Greater Than (>)
    // Expected: Cat 10 = 100 (row 1), Cat 20 = 400 (row 4)
    // ---------------------------------------------------------
    val sql2 = s"SELECT category, SUM(price) FROM $tableName WHERE stock > 10 GROUP BY category"
    val res2 = SQLHandler.execute(sql2)
    assert(res2.contains("10 | 100"))
    assert(res2.contains("20 | 400"))
    assert(!res2.contains("30 |"))

    // ---------------------------------------------------------
    // TEST 3: Greater Than or Equal (>=)
    // Expected: Cat 10 = 300 (rows 1 & 2), Cat 20 = 400 (row 4)
    // ---------------------------------------------------------
    val sql3 = s"SELECT category, SUM(price) FROM $tableName WHERE stock >= 10 GROUP BY category"
    val res3 = SQLHandler.execute(sql3)
    assert(res3.contains("10 | 300"))
    assert(res3.contains("20 | 400"))

    // ---------------------------------------------------------
    // TEST 4: Minor Than (<)
    // Expected: Cat 20 = 300 (row 3), Cat 30 = 500 (row 5)
    // ---------------------------------------------------------
    val sql4 = s"SELECT category, SUM(price) FROM $tableName WHERE stock < 10 GROUP BY category"
    val res4 = SQLHandler.execute(sql4)
    assert(!res4.contains("10 |")) 
    assert(res4.contains("20 | 300"))
    assert(res4.contains("30 | 500"))

    // ---------------------------------------------------------
    // TEST 5: Minor Than or Equal (<=)
    // Expected: Cat 20 = 300 (row 3), Cat 30 = 500 (row 5)
    // ---------------------------------------------------------
    val sql5 = s"SELECT category, SUM(price) FROM $tableName WHERE stock <= 5 GROUP BY category"
    val res5 = SQLHandler.execute(sql5)
    assert(res5.contains("20 | 300"))
    assert(res5.contains("30 | 500"))

    // ---------------------------------------------------------
    // TEST 6: Equals To (=)
    // Expected: Cat 30 = 500 (row 5)
    // ---------------------------------------------------------
    val sql6 = s"SELECT category, SUM(price) FROM $tableName WHERE stock = 0 GROUP BY category"
    val res6 = SQLHandler.execute(sql6)
    assert(!res6.contains("10 |"))
    assert(!res6.contains("20 |"))
    assert(res6.contains("30 | 500"))

    // ---------------------------------------------------------
    // TEST 7: Empty Result Handling
    // Expected: Empty / No crash
    // ---------------------------------------------------------
    val sql7 = s"SELECT category, SUM(price) FROM $tableName WHERE stock > 1000 GROUP BY category"
    val res7 = SQLHandler.execute(sql7)
    assert(res7.contains("Empty"))

    // Cleanup runtime memory AND disk storage
    salesTable.close()
    deleteRecursively(testDirFile)
  }
}