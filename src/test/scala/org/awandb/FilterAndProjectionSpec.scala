/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import java.io.File

class FilterAndProjectionSpec extends AnyFunSuite with BeforeAndAfterAll {

  val dataDir = "data_filters_test"
  var table: AwanTable = _

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  override def beforeAll(): Unit = {
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()

    table = new AwanTable("catalog", 1000, dataDir)
    table.addColumn("id")        // 0
    table.addColumn("price")     // 1
    table.addColumn("stock")     // 2
    table.addColumn("category")  // 3
    
    SQLHandler.register("catalog", table)
    
    // --- DISK BATCH (Will be scanned by C++ AVX2) ---
    table.insertRow(Array(1, 100, 50, 10))
    table.insertRow(Array(2, 200, 0,  10))
    table.insertRow(Array(3, 150, 20, 20))
    table.insertRow(Array(4, 50,  5,  20))
    table.insertRow(Array(5, 300, 100, 30))
    table.flush() // Force to BlockManager
    
    // --- RAM BATCH (Will be scanned by Scala Iterator) ---
    table.insertRow(Array(6, 300, 0,  30))
    table.insertRow(Array(7, 500, 10, 40))
    table.insertRow(Array(8, 50,  50, 40))
    table.insertRow(Array(9, 100, 10, 10))
    table.insertRow(Array(10, 1000, 1, 50))
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // 1. EXTENSIVE PROJECTION TESTS
  // -------------------------------------------------------------------------

  test("1. Projection: Should support SELECT * fallback") {
    val sql = "SELECT * FROM catalog WHERE id = 1"
    val res = SQLHandler.execute(sql)
    assert(res.contains("1 | 100 | 50 | 10"), "Failed to project all columns")
  }

  test("2. Projection: Should isolate a single column") {
    val sql = "SELECT price FROM catalog WHERE id = 1"
    val res = SQLHandler.execute(sql)
    assert(res.contains("100"), "Failed to project single column")
    assert(!res.contains("50 | 10"), "Leaked extra columns")
  }

  test("3. Projection: Should handle out-of-order schema requests") {
    val sql = "SELECT category, id, stock FROM catalog WHERE id = 1"
    val res = SQLHandler.execute(sql)
    // Expected: category(10) | id(1) | stock(50)
    assert(res.contains("10 | 1 | 50"), "Failed to project out-of-order columns")
  }

  test("4. Projection: Should safely reject non-existent columns") {
    val sql = "SELECT id, ghost_col FROM catalog WHERE id = 1"
    val res = SQLHandler.execute(sql)
    assert(res.contains("Error: One or more projected columns do not exist"), "Did not catch missing column")
  }

  // -------------------------------------------------------------------------
  // 2. PREDICATE PUSHDOWN (AVX FILTER) TESTS
  // -------------------------------------------------------------------------

  test("5. Filter: Should support exact match (=) with multiple results") {
    // Both row 1 (Disk) and row 9 (RAM) have price = 100
    val sql = "SELECT id, price FROM catalog WHERE price = 100"
    val res = SQLHandler.execute(sql)
    assert(res.contains("1 | 100"), "Failed to match Disk row")
    assert(res.contains("9 | 100"), "Failed to match RAM row")
  }

  test("6. Filter: Should support Greater Than (>) crossing RAM/Disk boundary") {
    val sql = "SELECT id, price FROM catalog WHERE price > 300"
    val res = SQLHandler.execute(sql)
    assert(res.contains("7 | 500"), "Missed row 7")
    assert(res.contains("10 | 1000"), "Missed row 10")
    assert(!res.contains("300"), "Incorrectly included boundary value (300)")
  }

  test("7. Filter: Should support Greater Than or Equal (>=) with duplicates") {
    // Rows 5 (Disk) and 6 (RAM) have price exactly 300
    val sql = "SELECT id, price FROM catalog WHERE price >= 300"
    val res = SQLHandler.execute(sql)
    assert(res.contains("5 | 300"), "Missed Disk boundary row")
    assert(res.contains("6 | 300"), "Missed RAM boundary row")
    assert(res.contains("10 | 1000"), "Missed larger row")
  }

  test("8. Filter: Should support Less Than (<)") {
    val sql = "SELECT id, price FROM catalog WHERE price < 100"
    val res = SQLHandler.execute(sql)
    assert(res.contains("4 | 50"), "Missed row 4")
    assert(res.contains("8 | 50"), "Missed row 8")
    assert(!res.contains("100"), "Incorrectly included boundary value (100)")
  }

  test("9. Filter: Should support Less Than or Equal (<=)") {
    val sql = "SELECT id, stock FROM catalog WHERE stock <= 0"
    val res = SQLHandler.execute(sql)
    assert(res.contains("2 | 0"), "Missed Disk row with 0 stock")
    assert(res.contains("6 | 0"), "Missed RAM row with 0 stock")
    assert(!res.contains("1"), "Included row > 0")
  }

  // -------------------------------------------------------------------------
  // 3. ADVANCED INTEGRATION TESTS
  // -------------------------------------------------------------------------

  test("10. Integration: Should filter on a column NOT in the SELECT clause") {
    // Query category, but filter by stock
    val sql = "SELECT id, category FROM catalog WHERE stock = 100"
    val res = SQLHandler.execute(sql)
    // Row 5 has stock 100 and category 30
    assert(res.contains("5 | 30"), "Failed to filter on non-projected column")
  }

  test("11. Integration: AVX Bitmask Pushdown (Should ignore deleted rows)") {
    // 1. Verify row 2 exists and matches filter
    val check1 = SQLHandler.execute("SELECT id FROM catalog WHERE stock = 0")
    assert(check1.contains("   2"), "Pre-condition failed: Row 2 missing")
    
    // 2. Delete Row 2 (This modifies the C++ BitSet for Block 0)
    SQLHandler.execute("DELETE FROM catalog WHERE id = 2")
    
    // 3. Run the filter again. The AVX vector engine should skip row 2 natively.
    val check2 = SQLHandler.execute("SELECT id FROM catalog WHERE stock = 0")
    assert(!check2.contains("   2"), "AVX Predicate Pushdown failed to respect the deletion bitmask!")
    assert(check2.contains("   6"), "AVX Predicate dropped valid rows after a deletion")
  }

  test("12. Filter: Should reject unsupported WHERE clauses gracefully") {
    val sql = "SELECT * FROM inventory WHERE price BETWEEN 100 AND 200"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("Error") || res.contains("Unsupported"), "Did not catch unsupported operator")
  }
}