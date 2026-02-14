/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import java.io.File

class ComplexSQLSpec extends AnyFunSuite with BeforeAndAfterAll {

  val dataDir = "data_complex_sql"
  var ordersTable: AwanTable = _
  var customersTable: AwanTable = _
  var emptyTable: AwanTable = _

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

    // 1. Setup Customers Table (Right Side of Join)
    customersTable = new AwanTable("customers", 1000, dataDir)
    customersTable.addColumn("id")      // PK
    customersTable.addColumn("loyalty") // Points
    SQLHandler.register("customers", customersTable)
    
    customersTable.insertRow(Array(1, 500))
    customersTable.insertRow(Array(2, 1000))
    customersTable.insertRow(Array(3, 0)) // Customer with no orders

    // 2. Setup Orders Table (Left Side of Join)
    ordersTable = new AwanTable("orders", 1000, dataDir)
    ordersTable.addColumn("order_id")    // PK
    ordersTable.addColumn("customer_id") // FK -> customers.id
    ordersTable.addColumn("amount")      // Order Value
    SQLHandler.register("orders", ordersTable)
    
    ordersTable.insertRow(Array(101, 1, 250))
    ordersTable.insertRow(Array(102, 1, 300))
    ordersTable.insertRow(Array(103, 2, 900))
    ordersTable.insertRow(Array(104, 99, 50)) // Orphan order (Customer 99 doesn't exist)

    // 3. Setup Empty Table (For Edge Cases)
    emptyTable = new AwanTable("empty_logs", 1000, dataDir)
    emptyTable.addColumn("log_id")
    emptyTable.addColumn("severity")
    SQLHandler.register("empty_logs", emptyTable)
    
    // Flush to disk to test native scanning
    customersTable.flush()
    ordersTable.flush()
  }

  override def afterAll(): Unit = {
    if (ordersTable != null) ordersTable.close()
    if (customersTable != null) customersTable.close()
    if (emptyTable != null) emptyTable.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // 1. AGGREGATION (GROUP BY) TESTS
  // -------------------------------------------------------------------------

  test("1. Aggregation: Should execute GROUP BY and SUM() correctly") {
    val sql = "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("1 | 550"), "Customer 1 should sum to 550")
    assert(res.contains("2 | 900"), "Customer 2 should sum to 900")
    assert(res.contains("99 | 50"), "Customer 99 should sum to 50")
  }

  test("2. Aggregation: Should handle GROUP BY on an empty table gracefully") {
    val sql = "SELECT log_id, SUM(severity) FROM empty_logs GROUP BY log_id"
    val res = SQLHandler.execute(sql)
    
    assert(!res.contains("SQL Error"), "Empty table aggregation should not throw exceptions")
    // Map should just be empty, returning the header but no rows
    assert(res.contains("GROUP BY Results") && !res.contains("|")) 
  }

  test("3. Aggregation: Should reject GROUP BY on missing columns") {
    val sql = "SELECT ghost_col, SUM(amount) FROM orders GROUP BY ghost_col"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("SQL Error: Column not found"), "Engine did not catch missing group-by column")
  }

  test("4. Aggregation: Should reject GROUP BY without a SUM function") {
    val sql = "SELECT customer_id FROM orders GROUP BY customer_id"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("Error: GROUP BY currently requires a SUM(column) aggregate"), "Engine did not enforce MVP SUM requirement")
  }

  // -------------------------------------------------------------------------
  // 2. INNER JOIN TESTS
  // -------------------------------------------------------------------------

  test("5. Join: Should execute INNER JOIN and map 1-to-N relationships") {
    val sql = "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
    val res = SQLHandler.execute(sql)
    
    // Order 101 (Customer 1) -> Loyalty 500
    assert(res.contains("LeftKey: 1 | RightPayload: 500"), "Join failed on Order 101")
    // Order 102 (Customer 1) -> Loyalty 500
    assert(res.contains("LeftKey: 1 | RightPayload: 500"), "Join failed on Order 102")
    // Order 103 (Customer 2) -> Loyalty 1000
    assert(res.contains("LeftKey: 2 | RightPayload: 1000"), "Join failed on Order 103")
    
    // Total matches should exactly equal the number of valid pairs
    assert(res.contains("Matches Found: 3"), "Expected exactly 3 joined rows")
  }

  test("6. Join: Should ignore orphan rows on the left (No match on right)") {
    val sql = "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
    val res = SQLHandler.execute(sql)
    
    // Order 104 belongs to Customer 99, which doesn't exist in the customers table.
    // In an INNER JOIN, this row should be dropped.
    assert(!res.contains("LeftKey: 99"), "INNER JOIN incorrectly included an orphan row")
  }

  test("7. Join: Should ignore unmatched rows on the right") {
    val sql = "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
    val res = SQLHandler.execute(sql)
    
    // Customer 3 has no orders, so their payload (0 loyalty) should not appear in the join result.
    assert(!res.contains("RightPayload: 0"), "INNER JOIN incorrectly included unmatched right-side row")
  }

  test("8. Join: Should reject JOIN on a non-existent table") {
    val sql = "SELECT * FROM orders JOIN ghost_table ON orders.customer_id = ghost_table.id"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("Error: Join Table 'ghost_table' not found"), "Engine failed to catch missing join table")
  }
}