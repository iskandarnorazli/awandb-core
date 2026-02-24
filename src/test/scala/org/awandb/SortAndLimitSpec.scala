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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import java.io.File
import org.awandb.TestHelpers.given

class SortAndLimitSpec extends AnyFunSuite with BeforeAndAfterAll {

  val dataDir = "data_sort_test"
  var table: AwanTable = _
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

    table = new AwanTable("leaderboard", 1000, dataDir)
    table.addColumn("id")      
    table.addColumn("player_id") 
    table.addColumn("score")
    
    SQLHandler.register("leaderboard", table)
    
    // Insert test data
    table.insertRow(Array(1, 101, 150))
    table.insertRow(Array(2, 102, 900))
    table.insertRow(Array(3, 103, 250))
    table.insertRow(Array(4, 104, 500)) 
    table.insertRow(Array(5, 105, 900)) 
    
    table.flush()

    // 2. Empty Table
    emptyTable = new AwanTable("empty_board", 1000, dataDir)
    emptyTable.addColumn("id")
    emptyTable.addColumn("score")
    SQLHandler.register("empty_board", emptyTable)
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    if (emptyTable != null) emptyTable.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // Helper method to parse rows safely whether they have 1 column or N columns
  private def parseRows(res: String): Array[String] = {
    res.split("\n")
       .map(_.trim)
       // [FIX] Use startsWith to handle dynamic column headers like "Found Rows (score):"
       .filter(s => s.nonEmpty && !s.startsWith("Found Rows")) 
  }

  // -------------------------------------------------------------------------
  // 1. EXTENSIVE ORDER BY TESTS
  // -------------------------------------------------------------------------

  test("1. Sort: Should order by Integer column ASCENDING") {
    val sql = "SELECT score FROM leaderboard ORDER BY score ASC"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows(0) == "150", "First row should be 150")
    assert(rows(4) == "900", "Last row should be 900")
  }

  test("2. Sort: Should order by Integer column DESCENDING") {
    val sql = "SELECT score FROM leaderboard ORDER BY score DESC"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows(0) == "900", "First row should be 900")
    assert(rows(4) == "150", "Last row should be 150")
  }

  test("3. Sort: Should handle sorting an empty table gracefully") {
    val sql = "SELECT score FROM empty_board ORDER BY score DESC"
    val res = SQLHandler.execute(sql)
    
    assert(!res.contains("SQL Error"), "Threw exception on empty table sort")
    // [FIX] Removed the exact colon match
    assert(res.contains("Found Rows"), "Missing output header") 
  }

  // -------------------------------------------------------------------------
  // 2. EXTENSIVE LIMIT TESTS
  // -------------------------------------------------------------------------

  test("4. Limit: Should restrict standard results") {
    val sql = "SELECT * FROM leaderboard LIMIT 2"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows.length == 2, s"Expected 2 rows, got ${rows.length}")
  }

  test("5. Limit: Should handle LIMIT > Total Rows safely") {
    val sql = "SELECT * FROM leaderboard LIMIT 1000"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows.length == 5, s"Expected all 5 rows, got ${rows.length}")
  }

  test("6. Limit: Should handle LIMIT 0 by returning no rows") {
    val sql = "SELECT * FROM leaderboard LIMIT 0"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows.isEmpty, "LIMIT 0 should return exactly 0 rows")
  }

  // -------------------------------------------------------------------------
  // 3. THE ULTIMATE PIPELINE: WHERE + ORDER BY + LIMIT
  // -------------------------------------------------------------------------

  test("7. Pipeline: Should evaluate AVX WHERE, then Sort, then Limit") {
    val sql = "SELECT player_id, score FROM leaderboard WHERE score > 200 ORDER BY score ASC LIMIT 2"
    val res = SQLHandler.execute(sql)
    
    val rows = parseRows(res)
    assert(rows.length == 2, "Expected exactly 2 rows from the pipeline")
    assert(rows(0).contains("103") && rows(0).contains("250"), s"Rank 1 is incorrect: ${rows(0)}")
    assert(rows(1).contains("104") && rows(1).contains("500"), s"Rank 2 is incorrect: ${rows(1)}")
  }
}