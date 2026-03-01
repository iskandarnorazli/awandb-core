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

package org.awandb.core.sql

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import java.io.File

class AISqlSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val testDir1 = "target/data_sql_vec"
  val testDir2 = "target/data_sql_graph"
  var vecTable: AwanTable = _
  var graphTable: AwanTable = _

  override def beforeEach(): Unit = {
    Seq(testDir1, testDir2).foreach { d =>
      val dir = new File(d)
      if (dir.exists()) dir.listFiles().foreach(_.delete())
      else dir.mkdirs()
    }
    
    vecTable = new AwanTable("docs", 1000, testDir1)
    vecTable.addColumn("id")
    vecTable.addColumn("embedding", isVector = true)
    SQLHandler.register("docs", vecTable)

    graphTable = new AwanTable("relationships", 1000, testDir2)
    graphTable.addColumn("id")
    graphTable.addColumn("src_id")
    graphTable.addColumn("dst_id")
    SQLHandler.register("relationships", graphTable)
  }

  override def afterEach(): Unit = {
    vecTable.close()
    graphTable.close()
    Seq(testDir1, testDir2).foreach { d =>
      val dir = new File(d)
      if (dir.exists()) dir.listFiles().foreach(_.delete())
    }
  }

  test("SQLHandler should route VECTOR_SEARCH in WHERE clause to Native AVX Engine") {
    vecTable.insertRow(Array(10, Array(1.0f, 0.0f, 0.0f)))
    vecTable.insertRow(Array(20, Array(0.0f, 1.0f, 0.0f)))
    vecTable.flush()

    val sql = "SELECT * FROM docs WHERE VECTOR_SEARCH(embedding, '[1.0, 0.0, 0.0]', 0.9) = 1"
    val result = SQLHandler.execute(sql)

    result.isError shouldBe false
    result.affectedRows shouldBe 1
    
    // [FIX] Use exact boundaries to prevent floating point memory overlap matches
    result.message should include("   10 |")
    result.message should not include("   20 |")
  }

  test("SQLHandler should route BFS_DISTANCE in SELECT clause to Native Graph Engine") {
    graphTable.insertRow(Array(1, 0, 1))
    graphTable.insertRow(Array(2, 0, 2))
    graphTable.insertRow(Array(3, 2, 3))
    graphTable.flush()

    val sql = "SELECT BFS_DISTANCE(0) FROM relationships"
    val result = SQLHandler.execute(sql)

    result.isError shouldBe false
    result.affectedRows shouldBe 4 

    result.message should include("0 | 0") 
    result.message should include("1 | 1") 
    result.message should include("2 | 1") 
    result.message should include("3 | 2") 
  }

  test("SQLHandler should combine VECTOR_SEARCH with standard Relational filters (AND/OR)") {
    vecTable.insertRow(Array(10, Array(1.0f, 0.0f, 0.0f))) 
    vecTable.insertRow(Array(20, Array(1.0f, 0.0f, 0.0f))) 
    vecTable.insertRow(Array(30, Array(0.0f, 1.0f, 0.0f))) 
    vecTable.flush()

    // Test AND intersection
    val sqlAnd = "SELECT * FROM docs WHERE VECTOR_SEARCH(embedding, '[1.0, 0.0, 0.0]', 0.9) = 1 AND id > 15"
    val resultAnd = SQLHandler.execute(sqlAnd)

    resultAnd.isError shouldBe false
    resultAnd.affectedRows shouldBe 1
    
    // [FIX] Use exact boundaries
    resultAnd.message should include("   20 |")
    resultAnd.message should not include("   10 |")
    resultAnd.message should not include("   30 |")

    // Test OR union
    val sqlOr = "SELECT * FROM docs WHERE VECTOR_SEARCH(embedding, '[1.0, 0.0, 0.0]', 0.9) = 1 OR id = 30"
    val resultOr = SQLHandler.execute(sqlOr)
    
    resultOr.isError shouldBe false
    resultOr.affectedRows shouldBe 3 
  }

  test("SQLHandler should gracefully handle BFS_DISTANCE on disconnected graphs and cycles") {
    graphTable.insertRow(Array(1, 0, 1))
    graphTable.insertRow(Array(2, 1, 0))
    graphTable.insertRow(Array(3, 5, 6))
    graphTable.flush()

    val sql = "SELECT BFS_DISTANCE(0) FROM relationships"
    val result = SQLHandler.execute(sql)

    result.isError shouldBe false
    result.affectedRows shouldBe 2 

    result.message should include("0 | 0")
    result.message should include("1 | 1")
    result.message should not include("5 |") 
    result.message should not include("6 |")
  }

  test("SQLHandler should gracefully reject invalid AI/Graph syntax without crashing") {
    val sqlBadCol = "SELECT * FROM docs WHERE VECTOR_SEARCH(bad_col, '[1.0, 0.0]', 0.9) = 1"
    val res1 = SQLHandler.execute(sqlBadCol)
    
    // [FIX] Will now return True and catch the RuntimeException correctly
    res1.isError shouldBe true
    res1.message should include("not found")

    val sqlBadVec = "SELECT * FROM docs WHERE VECTOR_SEARCH(embedding, 'not-a-vector', 0.9) = 1"
    val res2 = SQLHandler.execute(sqlBadVec)
    res2.isError shouldBe true
  }
}