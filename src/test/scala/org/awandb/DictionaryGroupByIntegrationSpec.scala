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

package org.awandb.core.sql

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import org.awandb.core.engine.memory.NativeMemoryTracker
import java.io.File

class DictionaryGroupByIntegrationSpec extends AnyFunSuite {

  def cleanDir(dir: File): Unit = {
    if (dir.exists()) {
      dir.listFiles().foreach(f => if (f.isDirectory) cleanDir(f) else f.delete())
      dir.delete()
    }
  }

  test("Phase 4: End-to-End SQL GROUP BY (Disk Only)") {
    val tableName = "sales_test"
    val dataDir = new File(s"data/$tableName")
    cleanDir(dataDir)

    val table = new AwanTable(tableName, capacity = 1000, dataDir = dataDir.getPath)
    table.addColumn("id") 
    table.addColumn("category", isString = true, useDictionary = true) 
    table.addColumn("amount")

    SQLHandler.register(tableName, table)

    table.insertRow(Array(1, "Electronics", 500))
    table.insertRow(Array(2, "Clothing", 200))
    table.insertRow(Array(3, "Electronics", 300)) // Electronics total = 800
    table.insertRow(Array(4, "Books", 50))
    table.insertRow(Array(5, "Clothing", 100))    // Clothing total = 300
    table.insertRow(Array(6, "Books", 150))       // Books total = 200

    table.flush() // Force everything to C++ blocks

    val sql = "SELECT category, SUM(amount) FROM sales_test GROUP BY category"
    val result = SQLHandler.execute(sql)

    assert(!result.isError, s"SQL Execution failed: ${result.message}")
    assert(result.affectedRows == 3, "Should return exactly 3 grouped rows")

    val keys = result.columnarData(0).asInstanceOf[Array[Int]]
    val vals = result.columnarData(1).asInstanceOf[Array[String]].map(_.toLong)
    val resultMap = keys.zip(vals).toMap

    val electronicsId = table.columns("category").getDictId("Electronics")
    val clothingId = table.columns("category").getDictId("Clothing")
    val booksId = table.columns("category").getDictId("Books")

    assert(resultMap(electronicsId) == 800L)
    assert(resultMap(clothingId) == 300L)
    assert(resultMap(booksId) == 200L)

    table.close()
    cleanDir(dataDir)
    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: Mixed RAM and Disk Dictionary Aggregation") {
    val tableName = "sales_mixed"
    val dataDir = new File(s"data/$tableName")
    cleanDir(dataDir)

    val table = new AwanTable(tableName, capacity = 1000, dataDir = dataDir.getPath)
    table.addColumn("id") 
    table.addColumn("category", isString = true, useDictionary = true) 
    table.addColumn("amount")

    SQLHandler.register(tableName, table)

    // Block 1 (Goes to Disk)
    table.insertRow(Array(1, "Electronics", 500))
    table.insertRow(Array(2, "Clothing", 200))
    table.flush()

    // Block 2 (Stays in RAM Delta Store - Unflushed)
    table.insertRow(Array(3, "Electronics", 300)) // Electronics total = 800
    table.insertRow(Array(4, "Books", 50))
    table.insertRow(Array(5, "Clothing", 100))    // Clothing total = 300

    // The SQL Router should trigger AwanTable.executeDictionaryGroupBy
    // which must successfully merge the C++ Map and the JVM Map
    val sql = "SELECT category, SUM(amount) FROM sales_mixed GROUP BY category"
    val result = SQLHandler.execute(sql)

    assert(!result.isError, s"SQL Execution failed: ${result.message}")
    assert(result.affectedRows == 3)

    val keys = result.columnarData(0).asInstanceOf[Array[Int]]
    val vals = result.columnarData(1).asInstanceOf[Array[String]].map(_.toLong)
    val resultMap = keys.zip(vals).toMap

    val electronicsId = table.columns("category").getDictId("Electronics")
    val clothingId = table.columns("category").getDictId("Clothing")
    val booksId = table.columns("category").getDictId("Books")

    assert(resultMap(electronicsId) == 800L)
    assert(resultMap(clothingId) == 300L)
    assert(resultMap(booksId) == 50L)

    table.close()
    cleanDir(dataDir)
    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: COUNT aggregate with HAVING, ORDER BY, and LIMIT") {
    val tableName = "sales_complex"
    val dataDir = new File(s"data/$tableName")
    cleanDir(dataDir)

    val table = new AwanTable(tableName, capacity = 1000, dataDir = dataDir.getPath)
    table.addColumn("id") 
    table.addColumn("category", isString = true, useDictionary = true) 
    table.addColumn("amount")

    SQLHandler.register(tableName, table)

    // Add varying frequencies
    table.insertRow(Array(1, "Electronics", 10))
    table.insertRow(Array(2, "Electronics", 10))
    table.insertRow(Array(3, "Electronics", 10)) // Count = 3 (ID 0)
    table.insertRow(Array(4, "Books", 10))
    table.insertRow(Array(5, "Books", 10))       // Count = 2 (ID 1)
    table.insertRow(Array(6, "Clothing", 10))    // Count = 1 (ID 2)

    table.flush()

    // 1. HAVING > 1 filters out Clothing.
    // 2. ORDER BY category DESC puts Books (ID 1) before Electronics (ID 0).
    // 3. LIMIT 1 cuts off Electronics.
    val sql = "SELECT category, COUNT(*) FROM sales_complex GROUP BY category HAVING COUNT(*) > 1 ORDER BY category DESC LIMIT 1"
    val result = SQLHandler.execute(sql)

    assert(!result.isError, s"SQL Execution failed: ${result.message}")
    assert(result.affectedRows == 1, "Should return exactly 1 row due to LIMIT")

    val keys = result.columnarData(0).asInstanceOf[Array[Int]]
    val vals = result.columnarData(1).asInstanceOf[Array[String]].map(_.toLong)

    val booksId = table.columns("category").getDictId("Books")
    assert(keys(0) == booksId, "First result should be Books based on DESC sort")
    assert(vals(0) == 2L, "Books count should be 2")

    table.close()
    cleanDir(dataDir)
    NativeMemoryTracker.assertNoLeaks()
  }
}