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

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.{SQLHandler, SQLResult}
import org.awandb.TestHelpers.given

class OrmCompatibilitySpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "orm_users"
  var table: AwanTable = _

  override def beforeAll(): Unit = {
    table = new AwanTable(tableName, 1000)
    table.addColumn("id")
    table.addColumn("age")
    SQLHandler.register(tableName, table)
  }

  override def afterAll(): Unit = {
    table.close()
  }

  test("1. Concurrency: Should return exact affected row counts") {
    // 1. Insert 1 row
    val res1 = SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 30)")
    assert(!res1.isError)
    assert(res1.affectedRows == 1L, "Insert should report exactly 1 affected row")

    // 2. Delete existing row
    val res2 = SQLHandler.execute(s"DELETE FROM $tableName WHERE id = 1")
    assert(!res2.isError)
    assert(res2.affectedRows == 1L, "Delete of existing row should report 1 affected row")

    // 3. Delete NON-existing row (Crucial for ORM Optimistic Concurrency)
    val res3 = SQLHandler.execute(s"DELETE FROM $tableName WHERE id = 999")
    assert(!res3.isError)
    assert(res3.affectedRows == 0L, "Delete of missing row MUST report 0 affected rows")

    // 4. Update NON-existing row
    val res4 = SQLHandler.execute(s"UPDATE $tableName SET age = 40 WHERE id = 999")
    assert(!res4.isError)
    assert(res4.affectedRows == 0L, "Update of missing row MUST report 0 affected rows")
  }

  test("2. Silent Error Trap: Should flag boolean isError instead of just strings") {
    val res1 = SQLHandler.execute("SELECT * FROM ghost_table")
    assert(res1.isError, "Missing table should set isError = true")

    val res2 = SQLHandler.execute("INVALID SQL SYNTAX;")
    assert(res2.isError, "Syntax errors should set isError = true")
  }

  test("3. ORM Feature: INSERT with RETURNING clause") {
    // ORMs need the database to echo the materialized row back to map into memory
    val res = SQLHandler.execute(s"INSERT INTO $tableName VALUES (2, 25) RETURNING *")
    
    assert(!res.isError, s"Engine rejected RETURNING syntax: ${res.message}")
    assert(res.affectedRows == 1L)
    
    // The response message should be formatted exactly like a SELECT query output
    assert(res.message.contains("Found Rows:"), "RETURNING did not format output as a query result")
    assert(res.message.contains("2 | 25"), "RETURNING did not echo the inserted row data")
  }

  test("4. ORM Feature: DDL (CREATE TABLE) for schema migrations") {
    val createSql = """
      CREATE TABLE orm_products (
        id INT,
        name VARCHAR(255),
        price INT
      )
    """
    
    // 1. Execute the DDL Migration
    val createRes = SQLHandler.execute(createSql)
    assert(!createRes.isError, s"Engine rejected CREATE TABLE syntax: ${createRes.message}")
    
    // 2. Verify the table was actually registered in the Engine
    val tableExists = SQLHandler.tables.containsKey("orm_products")
    assert(tableExists, "CREATE TABLE succeeded but table was not registered in SQLHandler")
    
    // 3. Verify the engine created the columns correctly by inserting data
    val insertRes = SQLHandler.execute("INSERT INTO orm_products VALUES (1, 'Mechanical Keyboard', 150)")
    assert(!insertRes.isError, "Failed to insert into newly created table")
    
    // 4. Verify data types are correct (String vs Int)
    val selectRes = SQLHandler.execute("SELECT * FROM orm_products")
    assert(selectRes.message.contains("1 | Mechanical Keyboard | 150"), "Dynamic schema failed to maintain data types")
  }

  test("5. ORM Feature: DDL (DROP TABLE) for schema teardown") {
    // 1. Create a dummy table
    SQLHandler.execute("CREATE TABLE to_be_dropped (id INT)")
    assert(SQLHandler.tables.containsKey("to_be_dropped"))

    // 2. Drop the table
    val dropRes = SQLHandler.execute("DROP TABLE to_be_dropped")
    assert(!dropRes.isError, s"Engine rejected DROP TABLE syntax: ${dropRes.message}")

    // 3. Verify it was completely removed from the engine
    assert(!SQLHandler.tables.containsKey("to_be_dropped"), "Table was not removed from the registry")
    
    // 4. Verify querying it now throws a proper error
    val queryRes = SQLHandler.execute("SELECT * FROM to_be_dropped")
    assert(queryRes.isError, "Querying a dropped table should return an error")
  }

  test("6. ORM Feature: DDL (ALTER TABLE) for schema evolution") {
    // 1. Create a base table
    SQLHandler.execute("CREATE TABLE alter_test (id INT)")

    // 2. Alter the table to add a new string column
    val alterRes = SQLHandler.execute("ALTER TABLE alter_test ADD COLUMN status VARCHAR(255)")
    assert(!alterRes.isError, s"Engine rejected ALTER TABLE syntax: ${alterRes.message}")

    // 3. Verify the engine dynamically adjusted the physical schema by inserting a wide row
    val insertRes = SQLHandler.execute("INSERT INTO alter_test VALUES (1, 'active')")
    assert(!insertRes.isError, s"Failed to insert into altered table: ${insertRes.message}")

    // 4. Verify the new column is projected correctly
    val selectRes = SQLHandler.execute("SELECT * FROM alter_test")
    assert(selectRes.message.contains("1 | active"), "Failed to project newly added column")
  }

  test("7. ORM Feature: Unrestricted DML (UPDATE / DELETE with dynamic WHERE)") {
    SQLHandler.execute("CREATE TABLE orm_dml (id INT, age INT, score INT)")
    SQLHandler.execute("INSERT INTO orm_dml VALUES (1, 20, 100)")
    SQLHandler.execute("INSERT INTO orm_dml VALUES (2, 35, 100)")
    SQLHandler.execute("INSERT INTO orm_dml VALUES (3, 40, 100)")

    // 1. Unrestricted UPDATE (Multiple rows)
    val updateRes = SQLHandler.execute("UPDATE orm_dml SET score = 0 WHERE age > 30")
    assert(!updateRes.isError, s"Engine rejected complex UPDATE: ${updateRes.message}")
    assert(updateRes.affectedRows == 2L, s"Should have updated exactly 2 rows, got ${updateRes.affectedRows}")

    // Verify update applied correctly
    val selectUpdate = SQLHandler.execute("SELECT * FROM orm_dml WHERE score = 0")
    assert(selectUpdate.message.contains("2 | 35 | 0"))
    assert(selectUpdate.message.contains("3 | 40 | 0"))

    // 2. Unrestricted DELETE (Multiple rows)
    val deleteRes = SQLHandler.execute("DELETE FROM orm_dml WHERE score = 0")
    assert(!deleteRes.isError, s"Engine rejected complex DELETE: ${deleteRes.message}")
    assert(deleteRes.affectedRows == 2L, s"Should have deleted exactly 2 rows, got ${deleteRes.affectedRows}")

    // Verify delete applied correctly
    val selectDelete = SQLHandler.execute("SELECT * FROM orm_dml")
    assert(selectDelete.message.contains("1 | 20 | 100"), "Row 1 should still exist")
    assert(!selectDelete.message.contains("35"), "Row 2 should be deleted")
    assert(!selectDelete.message.contains("40"), "Row 3 should be deleted")
  }
}