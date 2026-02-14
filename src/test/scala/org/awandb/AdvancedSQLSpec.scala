/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import java.io.File

class AdvancedSQLSpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "secure_products"
  val dataDir = "data_advanced_test"
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

    table = new AwanTable(tableName, 1000, dataDir)
    table.addColumn("id")    
    table.addColumn("price") 
    table.addColumn("stock") 
    
    SQLHandler.register(tableName, table)
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // 1. PERSISTENCE & DURABILITY TEST
  // -------------------------------------------------------------------------

  test("1. Persistence: Data should survive a database restart") {
    // Step 1: Insert Data
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (99, 500, 10)")
    val select1 = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 99")
    assert(select1.contains("99 | 500 | 10"), "Data should be in RAM")

    // Step 2: Flush to Disk
    table.flush()

    // Step 3: Simulate Server Crash / Restart
    table.close() // Close file handles and free native memory
    
    // Create a BRAND NEW table instance pointing to the same directory
    val recoveredTable = new AwanTable(tableName, 1000, dataDir)
    recoveredTable.addColumn("id")
    recoveredTable.addColumn("price")
    recoveredTable.addColumn("stock")
    
    // Re-register to SQL Handler
    SQLHandler.register(tableName, recoveredTable)

    // Step 4: Try to SELECT the data again
    val select2 = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 99")
    
    // Cleanup the recovered table so afterAll() doesn't fail
    table = recoveredTable 

    assert(select2.contains("99 | 500 | 10"), "Data did not survive the restart! BlockManager/WAL recovery failed.")
  }

  // -------------------------------------------------------------------------
  // 2. SECURITY & SQL INJECTION TESTS
  // -------------------------------------------------------------------------

  test("2. Security: Should reject classic OR 1=1 injection") {
    // Attempt to bypass the WHERE clause
    val maliciousQuery = s"UPDATE $tableName SET price = 0 WHERE id = 99 OR 1=1"
    val res = SQLHandler.execute(maliciousQuery)
    
    // Because we strictly pattern match on `EqualsTo` in SQLHandler, 
    // JSqlParser will parse this as an `OrExpression` and it will fall through to our default error.
    assert(res.contains("Unsupported SQL statement") || res.contains("UPDATE only supports 'WHERE id = value'"), 
      s"Engine accepted malicious query! Response: $res")
      
    // Verify the price wasn't actually changed to 0
    val check = SQLHandler.execute(s"SELECT * FROM $tableName WHERE id = 99")
    assert(!check.contains("99 | 0 | 10"), "SQL Injection succeeded and modified data!")
  }

  test("3. Security: Should reject stacked queries (DROP TABLE)") {
    // Attempt to run two statements at once
    val maliciousQuery = s"SELECT * FROM $tableName; DROP TABLE $tableName;"
    val res = SQLHandler.execute(maliciousQuery)
    
    // JSqlParser expects a single statement in CCJSqlParserUtil.parse().
    // Stacked queries usually throw a ParseException.
    assert(res.contains("SQL Error:"), "Engine did not catch stacked query syntax error!")
  }
  
  test("4. Security: Should handle invalid types gracefully") {
    // Provide a string where an integer is expected
    val res = SQLHandler.execute(s"INSERT INTO $tableName VALUES ('hacker', 500, 10)")
    
    // It should fail gracefully, NOT crash the JVM with a ClassCastException
    assert(res.contains("Error") || res.contains("Type not supported"), 
      s"Expected error message, but got: $res")
  }
}