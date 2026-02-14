/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.sql.SQLHandler
import org.awandb.core.engine.AwanTable
import java.io.File

class SQLHandlerSpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "test_products"
  val dataDir = "data_test"
  var table: AwanTable = _

  // Custom helper to replace the removed scala.reflect.io.Directory
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  override def beforeAll(): Unit = {
    // 1. Cleanup previous test data using standard java.io.File
    val dir = new File(dataDir)
    if (dir.exists()) {
      deleteRecursively(dir)
    }
    dir.mkdirs()

    // 2. Initialize Table
    table = new AwanTable(tableName, 1000, dataDir)
    table.addColumn("id")    // Col 0: PK
    table.addColumn("price") // Col 1
    table.addColumn("stock") // Col 2
    
    // 3. Register with SQL Engine
    SQLHandler.register(tableName, table)
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
  }

  // -------------------------------------------------------------------------
  // TEST CASES
  // -------------------------------------------------------------------------

  test("1. INSERT: Should add rows successfully") {
    val res1 = SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 100, 50)")
    assert(res1.contains("Inserted 1 row"))

    val res2 = SQLHandler.execute(s"INSERT INTO $tableName VALUES (2, 200, 10)")
    assert(res2.contains("Inserted 1 row"))

    // Verify internal state (Direct API)
    assert(table.query("id", 1) == 1)
    assert(table.query("id", 2) == 1)
  }

  test("2. SELECT *: Should return all rows") {
    val res = SQLHandler.execute(s"SELECT * FROM $tableName")
    
    // We expect the output format defined in SQLHandler
    // "1 | 100 | 50"
    assert(res.contains("1 | 100 | 50"))
    assert(res.contains("2 | 200 | 10"))
  }

  test("3. SELECT WHERE: Should filter by ID") {
    val res = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 1")
    
    assert(res.contains("1 | 100 | 50"))
    assert(!res.contains("2 | 200 | 10"), "Should not contain Row 2")
  }

  test("4. UPDATE: Should modify 'price' and persist 'stock'") {
    // Action: Update Price of ID 1 from 100 -> 999
    val updateRes = SQLHandler.execute(s"UPDATE $tableName SET price = 999 WHERE id = 1")
    assert(updateRes.contains("Updated 1 row"))

    // Verify: Select row 1 again
    val selectRes = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 1")
    
    // Expected: 1 | 999 | 50  (Price changed, Stock remains 50)
    assert(selectRes.contains("1 | 999 | 50"))
    assert(!selectRes.contains("1 | 100 | 50"), "Old price should be gone")
  }

  test("5. DELETE: Should remove row") {
    val deleteRes = SQLHandler.execute(s"DELETE FROM $tableName WHERE id = 2")
    assert(deleteRes.contains("Deleted 1 row"))
    
    // Verify: Row 2 should be gone
    val selectRes = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 2")
    assert(!selectRes.contains("2 | 200"), "Row 2 should be deleted")
  }

  test("6. Error Handling: Non-existent table") {
    val res = SQLHandler.execute("SELECT * FROM ghost_table")
    assert(res.contains("Error: Table 'ghost_table' not found"))
  }
  
  test("7. Error Handling: Invalid SQL Syntax") {
    val res = SQLHandler.execute("INVALID COMMAND")
    assert(res.contains("SQL Error"))
  }
}