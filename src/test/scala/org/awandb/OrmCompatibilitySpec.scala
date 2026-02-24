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

  test("8. ORM Feature: Partial Inserts (Explicit Columns)") {
    SQLHandler.execute("CREATE TABLE orm_partial (id INT, name VARCHAR(255), stock INT)")

    // 1. Insert specifying ONLY id and name, leaving stock to default
    val insertRes = SQLHandler.execute("INSERT INTO orm_partial (id, name) VALUES (1, 'Laptop')")
    assert(!insertRes.isError, s"Engine rejected partial insert syntax: ${insertRes.message}")

    // 2. Verify the engine padded the missing column with a default value (0)
    val selectRes = SQLHandler.execute("SELECT * FROM orm_partial")
    assert(selectRes.message.contains("1 | Laptop | 0"), "Failed to pad missing columns with defaults")
  }

  test("9. ORM Feature: Table and Column Aliases") {
    SQLHandler.execute("CREATE TABLE orm_aliases (id INT, name VARCHAR(255), age INT)")
    SQLHandler.execute("INSERT INTO orm_aliases VALUES (1, 'Alice', 25)")
    SQLHandler.execute("INSERT INTO orm_aliases VALUES (2, 'Bob', 30)")

    // 1. Execute query heavily using aliases
    val selectRes = SQLHandler.execute("SELECT u.id AS user_id, u.name AS user_name FROM orm_aliases u WHERE u.age > 20")
    
    assert(!selectRes.isError, s"Engine rejected aliases: ${selectRes.message}")
    
    // 2. The engine should explicitly return the aliased headers
    assert(selectRes.message.contains("Found Rows (user_id | user_name):"), "Failed to project aliased column headers")
    assert(selectRes.message.contains("1 | Alice"))
    assert(selectRes.message.contains("2 | Bob"))
    
    SQLHandler.execute("DROP TABLE orm_aliases")
  }

  test("10. ORM Feature: Advanced Operators (IN, LIKE, IS NULL)") {
    SQLHandler.execute("CREATE TABLE orm_ops (id INT, name VARCHAR(255), age INT)")
    SQLHandler.execute("INSERT INTO orm_ops VALUES (1, 'Alice', 25)")
    SQLHandler.execute("INSERT INTO orm_ops VALUES (2, 'Bob', 30)")
    SQLHandler.execute("INSERT INTO orm_ops VALUES (3, 'Charlie', 35)")
    // Insert partial row to trigger a default padding (which acts as NULL in AwanDB)
    SQLHandler.execute("INSERT INTO orm_ops (id, name) VALUES (4, 'Dave')") 

    // 1. IN operator
    val inRes = SQLHandler.execute("SELECT * FROM orm_ops WHERE id IN (1, 3)")
    assert(!inRes.isError, s"Engine rejected IN syntax: ${inRes.message}")
    assert(inRes.message.contains("1 | Alice"))
    assert(inRes.message.contains("3 | Charlie"))
    assert(!inRes.message.contains("2 | Bob"))

    // 2. LIKE operator
    val likeRes = SQLHandler.execute("SELECT * FROM orm_ops WHERE name LIKE 'Al%'")
    assert(!likeRes.isError, s"Engine rejected LIKE syntax: ${likeRes.message}")
    assert(likeRes.message.contains("1 | Alice"))
    assert(!likeRes.message.contains("Bob"))

    // 3. IS NULL operator
    val nullRes = SQLHandler.execute("SELECT * FROM orm_ops WHERE age IS NULL")
    assert(!nullRes.isError, s"Engine rejected IS NULL syntax: ${nullRes.message}")
    assert(nullRes.message.contains("4 | Dave"))

    SQLHandler.execute("DROP TABLE orm_ops")
  }

  test("11. ORM Feature: Scalar Aggregations (COUNT, SUM, MAX, MIN, AVG)") {
    SQLHandler.execute("CREATE TABLE orm_agg (id INT, age INT, score INT)")
    SQLHandler.execute("INSERT INTO orm_agg VALUES (1, 20, 100)")
    SQLHandler.execute("INSERT INTO orm_agg VALUES (2, 30, 200)")
    SQLHandler.execute("INSERT INTO orm_agg VALUES (3, 40, 300)")

    // Aggregate with a WHERE clause (should only process Rows 2 and 3)
    val sql = "SELECT COUNT(*) AS total, SUM(score) AS sum_score, MAX(age) AS max_age, MIN(age) AS min_age, AVG(score) AS avg_score FROM orm_agg WHERE age > 20"
    val res = SQLHandler.execute(sql)
    
    assert(!res.isError, s"Engine rejected aggregations: ${res.message}")
    
    // Rows 2 & 3: Count = 2, Sum = 500, MaxAge = 40, MinAge = 30, Avg = 250.0
    assert(res.message.contains("Found Rows (total | sum_score | max_age | min_age | avg_score):"))
    assert(res.message.contains("2 | 500 | 40 | 30 | 250.0"), "Aggregations computed incorrect values")
    
    // Verify empty set handling (should return NULLs instead of crashing or returning Int.MaxValue)
    val emptyRes = SQLHandler.execute("SELECT MAX(score) FROM orm_agg WHERE age > 99")
    assert(emptyRes.message.contains("NULL"), "Empty sets should return NULL for MAX/MIN")
    
    SQLHandler.execute("DROP TABLE orm_agg")
  }

  test("12. ORM Feature: Complex Query (WHERE AND/OR, ORDER BY, LIMIT)") {
    SQLHandler.execute("CREATE TABLE orm_complex (id INT, name VARCHAR(255), score INT, category VARCHAR(255))")
    SQLHandler.execute("INSERT INTO orm_complex VALUES (1, 'Alice', 85, 'A')")
    SQLHandler.execute("INSERT INTO orm_complex VALUES (2, 'Bob', 92, 'B')")
    SQLHandler.execute("INSERT INTO orm_complex VALUES (3, 'Charlie', 78, 'A')")
    SQLHandler.execute("INSERT INTO orm_complex VALUES (4, 'Dave', 95, 'C')")
    SQLHandler.execute("INSERT INTO orm_complex VALUES (5, 'Eve', 88, 'B')")

    // Query: (Category A OR B) AND Score > 80, Ordered by Score DESC, Limit 2
    // Expected logic:
    // 1. Filtered: Alice (85, A), Bob (92, B), Eve (88, B)
    // 2. Sorted: Bob (92), Eve (88), Alice (85)
    // 3. Limited (2): Bob, Eve
    val sql = "SELECT * FROM orm_complex WHERE category IN ('A', 'B') AND score > 80 ORDER BY score DESC LIMIT 2"
    val res = SQLHandler.execute(sql)

    assert(!res.isError, s"Engine rejected complex query: ${res.message}")
    
    val lines = res.message.split("\n").filter(_.startsWith("   "))
    assert(lines.length == 3, "Output should contain exactly 1 header row and 2 data rows")
    assert(lines(1).contains("Bob | 92"), "First row should be Bob due to ORDER BY score DESC")
    assert(lines(2).contains("Eve | 88"), "Second row should be Eve")
    assert(!res.message.contains("Alice"), "Alice should be excluded by LIMIT 2")

    SQLHandler.execute("DROP TABLE orm_complex")
  }

  test("13. ORM Feature: Robust Error Handling for Invalid Complex Queries") {
    SQLHandler.execute("CREATE TABLE orm_err (id INT, val INT)")
    
    // 1. Filtering on a non-existent column
    val res1 = SQLHandler.execute("SELECT * FROM orm_err WHERE fake_col > 10")
    assert(res1.isError, "Engine must reject WHERE clauses on missing columns")

    // 2. Sorting on a non-existent column
    val res2 = SQLHandler.execute("SELECT * FROM orm_err ORDER BY fake_col DESC")
    assert(res2.isError, "Engine must reject ORDER BY on missing columns")

    // 3. GROUP BY constraint (Currently AwanDB only supports GROUP BY with SUM)
    val res3 = SQLHandler.execute("SELECT id, MAX(val) FROM orm_err GROUP BY id")
    assert(res3.isError)
    assert(res3.message.contains("requires a SUM(column)"), "Engine must return specific GROUP BY limitation error")

    // 4. Catastrophic Syntax Error
    val res4 = SQLHandler.execute("SELECT * FROM orm_err WHERE id = = AND OR 5")
    assert(res4.isError, "Engine must catch and wrap JSqlParser syntax errors")

    SQLHandler.execute("DROP TABLE orm_err")
  }

  test("14. ORM Feature: Advanced Scalar Aggregations with Deep Filtering") {
    SQLHandler.execute("CREATE TABLE orm_adv_agg (id INT, team VARCHAR(255), points INT, active INT)")
    SQLHandler.execute("INSERT INTO orm_adv_agg VALUES (1, 'Red', 10, 1)")
    SQLHandler.execute("INSERT INTO orm_adv_agg VALUES (2, 'Blue', 20, 1)")
    SQLHandler.execute("INSERT INTO orm_adv_agg VALUES (3, 'Red', 30, 0)")
    SQLHandler.execute("INSERT INTO orm_adv_agg VALUES (4, 'Green', 40, 1)")
    SQLHandler.execute("INSERT INTO orm_adv_agg VALUES (5, 'Red', 50, 1)")

    // Deep Filter: Active players in Red or Green team
    // active = 1 AND (team = 'Red' OR team = 'Green')
    // Valid rows -> Row 1 (10 pts), Row 4 (40 pts), Row 5 (50 pts)
    val sql = "SELECT COUNT(*) AS c, SUM(points) AS s, MAX(points) AS mx, MIN(points) AS mn, AVG(points) AS a FROM orm_adv_agg WHERE active = 1 AND team IN ('Red', 'Green')"
    val res = SQLHandler.execute(sql)
    
    assert(!res.isError, s"Engine rejected deep aggregate query: ${res.message}")

    // Expected: Count=3, Sum=100, Max=50, Min=10, Avg=33.333333333333336
    val expectedAvg = (100.toDouble / 3).toString 
    assert(res.message.contains(s"3 | 100 | 50 | 10 | $expectedAvg"), "Aggregations computed incorrect values over complex WHERE clause")

    SQLHandler.execute("DROP TABLE orm_adv_agg")
  }

  test("15. ORM Feature: Cross-verification of Complex Queries against Scala Collections") {
    SQLHandler.execute("CREATE TABLE orm_cross (id INT, val INT)")
    val random = new scala.util.Random(42)
    val data = (1 to 100).map(i => (i, random.nextInt(1000)))
    
    // Bulk load 100 random rows
    for ((id, v) <- data) {
       SQLHandler.execute(s"INSERT INTO orm_cross VALUES ($id, $v)")
    }

    // --- Phase 1: Verify Complex Pipeline (Filter + Sort + Limit) ---
    val expectedData = data.filter{ case (id, v) => id > 20 && id <= 80 && v > 500 }
                           .sortBy(-_._2) // Sort by val DESC
                           .take(5)

    val sql = "SELECT * FROM orm_cross WHERE id > 20 AND id <= 80 AND val > 500 ORDER BY val DESC LIMIT 5"
    val res = SQLHandler.execute(sql)
    assert(!res.isError, s"Engine rejected cross-validation query: ${res.message}")

    // Verify SQL engine output matches pure Scala collection transformations
    for ((expectedId, expectedVal) <- expectedData) {
       assert(res.message.contains(s"$expectedId | $expectedVal"), s"SQL Engine missed expected row $expectedId | $expectedVal")
    }

    // --- Phase 2: Verify Aggregation Math ---
    val aggData = data.filter{ case (id, v) => v < 500 }
    val expCount = aggData.size
    val expSum = aggData.map(_._2).sum
    val expMax = aggData.map(_._2).max
    val expMin = aggData.map(_._2).min
    val expAvg = expSum.toDouble / expCount

    val aggSql = "SELECT COUNT(*), SUM(val), MAX(val), MIN(val), AVG(val) FROM orm_cross WHERE val < 500"
    val aggRes = SQLHandler.execute(aggSql)
    assert(!aggRes.isError)
    
    // Match the SQL Engine's aggregated values against Scala's native math libraries
    assert(aggRes.message.contains(s"$expCount | $expSum | $expMax | $expMin | $expAvg"), "SQL Engine aggregation logic diverted from Scala standard math")

    SQLHandler.execute("DROP TABLE orm_cross")
  }
}