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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.awandb.core.sql.SQLHandler
import java.io.File

class AwanSQLRegressionSpec extends AnyFlatSpec with Matchers {

  // Helper to wipe test data between runs
  def clearDataDir(dir: String): Unit = {
    val f = new File(dir)
    if (f.exists()) {
      val files = f.listFiles()
      if (files != null) files.foreach(_.delete())
      f.delete()
    }
  }

  // Wrapper to assert no SQL parse/execution errors
  def execSafe(sql: String): org.awandb.core.sql.SQLResult = {
    val res = SQLHandler.execute(sql)
    if (res.isError) fail(s"SQL Failed: ${res.message} \nQuery: $sql")
    res
  }

  "SQL Engine" should "handle DDL operations (CREATE, ALTER, DROP)" in {
    val dir = "data/regression_ddl"
    clearDataDir(dir)

    // 1. DROP IF EXISTS (Safely handles missing tables)
    SQLHandler.execute("DROP TABLE regression_ddl") // Ignore error if it doesn't exist

    // 2. CREATE TABLE with all native types
    val resCreate = execSafe("CREATE TABLE regression_ddl (id INT, text_col STRING, embed VECTOR)")
    resCreate.affectedRows shouldBe 0L

    // 3. ALTER TABLE
    val resAlter = execSafe("ALTER TABLE regression_ddl ADD age INT")
    resAlter.affectedRows shouldBe 0L
    
    // Verify schema registered internally
    val table = SQLHandler.tables.get("regression_ddl")
    table should not be null
    table.columnOrder.toSeq shouldBe Seq("id", "text_col", "embed", "age")

    // 4. DROP TABLE
    val resDrop = execSafe("DROP TABLE regression_ddl")
    resDrop.affectedRows shouldBe 0L
    SQLHandler.tables.get("regression_ddl") shouldBe null
  }

  it should "execute standard CRUD operations with RETURNING clauses" in {
    val dir = "data/regression_crud"
    clearDataDir(dir)
    SQLHandler.execute("DROP TABLE regression_crud")

    execSafe("CREATE TABLE regression_crud (id INT, val INT, name STRING)")

    // 1. INSERT ... RETURNING
    val resIns = execSafe("INSERT INTO regression_crud (id, val, name) VALUES (1, 100, 'Alice') RETURNING")
    resIns.affectedRows shouldBe 1L
    resIns.message should include("Alice")

    // Test RETURNING * syntax too
    val resIns2 = execSafe("INSERT INTO regression_crud VALUES (2, 200, 'Bob') RETURNING *")
    resIns2.affectedRows shouldBe 1L
    resIns2.message should include("Bob")

    execSafe("INSERT INTO regression_crud VALUES (3, 300, 'Charlie')")

    // 2. UPDATE ... RETURNING
    val resUpd = execSafe("UPDATE regression_crud SET val = 999, name = 'Alice_Updated' WHERE id = 1 RETURNING")
    resUpd.affectedRows shouldBe 1L
    resUpd.message should include("Alice_Updated")

    // 3. DELETE
    val resDel = execSafe("DELETE FROM regression_crud WHERE id = 2")
    resDel.affectedRows shouldBe 1L

    // 4. SELECT validation
    val resSel = execSafe("SELECT id, val, name FROM regression_crud ORDER BY id ASC")
    resSel.affectedRows shouldBe 2L
    
    // Validate Columnar Transposition Array Output
    val idArr = resSel.columnarData(0).asInstanceOf[Array[Int]]
    val valArr = resSel.columnarData(1).asInstanceOf[Array[Int]]
    val nameArr = resSel.columnarData(2).asInstanceOf[Array[String]]
    
    idArr(0) shouldBe 1
    valArr(0) shouldBe 999
    nameArr(0) shouldBe "Alice_Updated"
    
    idArr(1) shouldBe 3
    valArr(1) shouldBe 300
    nameArr(1) shouldBe "Charlie"
  }

  it should "intercept and execute AVX Math updates natively" in {
    clearDataDir("data/regression_math")
    execSafe("CREATE TABLE regression_math (id INT, score INT)")
    
    execSafe("INSERT INTO regression_math VALUES (1, 10)")
    execSafe("INSERT INTO regression_math VALUES (2, 20)")
    execSafe("INSERT INTO regression_math VALUES (3, 30)")

    // Trigger Native AVX Subtraction & Multiplication
    execSafe("UPDATE regression_math SET score = score + 5 WHERE id > 0")
    execSafe("UPDATE regression_math SET score = score * 2 WHERE id = 3")

    val res = execSafe("SELECT score FROM regression_math ORDER BY id ASC")
    val scores = res.columnarData(0).asInstanceOf[Array[Int]]
    
    scores(0) shouldBe 15 // (10 + 5)
    scores(1) shouldBe 25 // (20 + 5)
    scores(2) shouldBe 70 // (30 + 5) * 2
  }

  it should "evaluate advanced WHERE predicates (LIKE, IN, IS NULL, AND, OR)" in {
    clearDataDir("data/regression_where")
    execSafe("CREATE TABLE regression_where (id INT, category INT, text_col STRING)")
    
    execSafe("INSERT INTO regression_where VALUES (1, 10, 'Apple')")
    execSafe("INSERT INTO regression_where VALUES (2, 20, 'Banana')")
    execSafe("INSERT INTO regression_where VALUES (3, 30, 'Apricot')")
    execSafe("INSERT INTO regression_where VALUES (4, 40, '')") // IS NULL equivalent for empty strings in AwanDB

    // LIKE
    val resLike = execSafe("SELECT id FROM regression_where WHERE text_col LIKE 'Ap%' ORDER BY id ASC")
    resLike.affectedRows shouldBe 2L
    resLike.columnarData(0).asInstanceOf[Array[Int]] shouldBe Array(1, 3)

    // IN list
    val resIn = execSafe("SELECT id FROM regression_where WHERE category IN (20, 40) ORDER BY id ASC")
    resIn.affectedRows shouldBe 2L
    resIn.columnarData(0).asInstanceOf[Array[Int]] shouldBe Array(2, 4)

    // IS NULL
    val resNull = execSafe("SELECT id FROM regression_where WHERE text_col IS NULL")
    resNull.affectedRows shouldBe 1L
    resNull.columnarData(0).asInstanceOf[Array[Int]](0) shouldBe 4

    // AND / OR pushdowns
    val resLogic = execSafe("SELECT id FROM regression_where WHERE (category > 15 AND category < 35) OR id = 1 ORDER BY id ASC")
    resLogic.affectedRows shouldBe 3L
    resLogic.columnarData(0).asInstanceOf[Array[Int]] shouldBe Array(1, 2, 3)
  }

  it should "process Aggregations (SUM, AVG, COUNT) with GROUP BY, HAVING, ORDER BY, LIMIT" in {
    clearDataDir("data/regression_agg")
    execSafe("CREATE TABLE regression_agg (id INT, department INT, salary INT)")
    
    execSafe("INSERT INTO regression_agg VALUES (1, 101, 5000)")
    execSafe("INSERT INTO regression_agg VALUES (2, 101, 7000)")
    execSafe("INSERT INTO regression_agg VALUES (3, 102, 4000)")
    execSafe("INSERT INTO regression_agg VALUES (4, 102, 4500)")
    execSafe("INSERT INTO regression_agg VALUES (5, 103, 10000)")

    // 1. Pure Aggregation
    val resPure = execSafe("SELECT COUNT(*), SUM(salary), MAX(salary) FROM regression_agg WHERE department > 101")
    resPure.columnarData(0).asInstanceOf[Array[String]](0) shouldBe "3" // Count
    resPure.columnarData(1).asInstanceOf[Array[String]](0) shouldBe "18500" // Sum
    resPure.columnarData(2).asInstanceOf[Array[String]](0) shouldBe "10000" // Max

    // 2. Complex Group By Pipeline
    val resGroup = execSafe(
      """SELECT department, SUM(salary) 
         FROM regression_agg 
         GROUP BY department 
         HAVING SUM(salary) > 8000 
         ORDER BY department DESC 
         LIMIT 1"""
    )
    
    resGroup.affectedRows shouldBe 1L
    
    val deptArr = resGroup.columnarData(0).asInstanceOf[Array[Int]]
    val sumArr = resGroup.columnarData(1).asInstanceOf[Array[String]]
    
    deptArr(0) shouldBe 103
    sumArr(0) shouldBe "10000"
  }

  it should "perform highly accurate AI VECTOR_SEARCH natively" in {
    clearDataDir("data/regression_vec")
    execSafe("CREATE TABLE regression_vec (id INT, embed VECTOR)")
    
    // Insert raw text representations of vectors
    execSafe("INSERT INTO regression_vec VALUES (1, '[1.0, 0.0, 0.0]')")
    execSafe("INSERT INTO regression_vec VALUES (2, '[0.0, 1.0, 0.0]')")
    execSafe("INSERT INTO regression_vec VALUES (3, '[0.9, 0.1, 0.0]')")
    execSafe("INSERT INTO regression_vec VALUES (4, '[0.0, 0.0, 1.0]')")

    // The threshold is set to 0.85, so only vectors heavily leaning towards X-axis should return
    val res = execSafe("SELECT id FROM regression_vec WHERE VECTOR_SEARCH(embed, '[1.0, 0.0, 0.0]', 0.85) ORDER BY id ASC")
    
    res.affectedRows shouldBe 2L
    val idArr = res.columnarData(0).asInstanceOf[Array[Int]]
    idArr should contain theSameElementsInOrderAs Array(1, 3)
  }

  it should "execute dynamic Graph BFS Traversals" in {
    clearDataDir("data/regression_graph")
    execSafe("CREATE TABLE regression_graph (id INT, src INT, dst INT)")
    
    execSafe("INSERT INTO regression_graph VALUES (1, 0, 1)")
    execSafe("INSERT INTO regression_graph VALUES (2, 1, 2)")
    execSafe("INSERT INTO regression_graph VALUES (3, 2, 3)")
    execSafe("INSERT INTO regression_graph VALUES (4, 99, 100)") // Disconnected subgraph

    // Execute BFS starting from Node 0
    val res = execSafe("SELECT BFS_DISTANCE(0, src, dst) FROM regression_graph")
    
    res.affectedRows shouldBe 4L // Nodes 0, 1, 2, 3 discovered
    res.message should include("0 | 0")
    res.message should include("1 | 1")
    res.message should include("2 | 2")
    res.message should include("3 | 3")
    res.message should not include("100 |") // Node 100 is unreachable
  }

  it should "execute INNER JOIN operations between native tables" in {
    clearDataDir("data/regression_join_l")
    clearDataDir("data/regression_join_r")
    
    execSafe("CREATE TABLE join_l (id INT, val INT)")
    execSafe("CREATE TABLE join_r (id INT, name STRING)")
    
    execSafe("INSERT INTO join_l VALUES (1, 100)")
    execSafe("INSERT INTO join_l VALUES (2, 200)")
    execSafe("INSERT INTO join_l VALUES (3, 300)") // Unmatched
    
    execSafe("INSERT INTO join_r VALUES (1, 'Alice')")
    execSafe("INSERT INTO join_r VALUES (2, 'Bob')")
    execSafe("INSERT INTO join_r VALUES (4, 'David')") // Unmatched

    val res = execSafe("SELECT join_l.id, join_r.name FROM join_l JOIN join_r ON join_l.id = join_r.id")
    
    res.affectedRows shouldBe 2L
    // Note: The JOIN engine currently materializes to `res.message` in the SQLHandler block.
    // The test validates that matches are successfully built and probed.
    res.message should include("Match -> LeftKey: 1")
    res.message should include("Match -> LeftKey: 2")
  }

  it should "execute multi-column UPDATEs including strings and AVX math without truncation" in {
    clearDataDir("data/regression_update")
    execSafe("CREATE TABLE regression_update (id INT, price INT, name STRING)")
    
    execSafe("INSERT INTO regression_update VALUES (1, 100, 'Alice')")
    execSafe("INSERT INTO regression_update VALUES (2, 200, 'Bob')")
    
    // 1. Math Update (Proves AVX mutator works natively)
    val resMath = execSafe("UPDATE regression_update SET price = price + 1000 WHERE id = 1 RETURNING")
    resMath.affectedRows shouldBe 1L
    resMath.message should include("1100")
    
    // 2. String Update (Proves the string quote fallback works)
    val resStr = execSafe("UPDATE regression_update SET name = 'Alice_Updated' WHERE id = 1 RETURNING")
    resStr.affectedRows shouldBe 1L
    resStr.message should include("Alice_Updated")

    // 3. Multi-Column Mixed Update (Proves JSqlParser doesn't truncate the column list!)
    val resMulti = execSafe("UPDATE regression_update SET price = price + 500, name = 'Bob_Updated' WHERE id = 2 RETURNING")
    resMulti.affectedRows shouldBe 1L
    resMulti.message should include("700")
    resMulti.message should include("Bob_Updated")
  }

  it should "correctly apply OLAP aggregations and evaluate LIMIT after aggregation" in {
    clearDataDir("data/regression_olap")
    execSafe("CREATE TABLE regression_olap (id INT, category INT, sales INT)")

    // Insert 5 rows totaling 500 in sales
    execSafe("INSERT INTO regression_olap VALUES (1, 10, 100)")
    execSafe("INSERT INTO regression_olap VALUES (2, 10, 100)")
    execSafe("INSERT INTO regression_olap VALUES (3, 20, 100)")
    execSafe("INSERT INTO regression_olap VALUES (4, 20, 100)")
    execSafe("INSERT INTO regression_olap VALUES (5, 20, 100)")

    // 1. The LIMIT bug trap: LIMIT 2 should NOT slice the base table to 2 rows before summing
    val resLimit = execSafe("SELECT SUM(sales) FROM regression_olap LIMIT 2")
    resLimit.columnarData(0).asInstanceOf[Array[String]](0) shouldBe "500" // If the bug exists, this will equal "200"

    // 2. The GROUP BY COUNT trap: Engine currently crashes expecting SUM
    val resGroup = execSafe("SELECT category, COUNT(*) as cnt FROM regression_olap GROUP BY category ORDER BY category ASC")
    resGroup.affectedRows shouldBe 2L
    
    val catArr = resGroup.columnarData(0).asInstanceOf[Array[Int]]
    val cntArr = resGroup.columnarData(1).asInstanceOf[Array[String]]
    catArr(0) shouldBe 10
    cntArr(0) shouldBe "2"
    catArr(1) shouldBe 20
    cntArr(1) shouldBe "3"
  }
}