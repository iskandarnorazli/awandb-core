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
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import java.io.File
import org.awandb.TestHelpers.given

class CompoundFilterSpec extends AnyFunSuite with BeforeAndAfterAll {

  val dataDir = "data_compound_test"
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

    table = new AwanTable("users", 1000, dataDir)
    table.addColumn("id")      
    table.addColumn("age")   
    table.addColumn("score")
    table.addColumn("active") // 1 for true, 0 for false
    
    SQLHandler.register("users", table)
    
    // Insert test data
    table.insertRow(Array(1, 20, 100, 1))
    table.insertRow(Array(2, 30, 500, 1))
    table.insertRow(Array(3, 25, 200, 0))
    table.insertRow(Array(4, 40, 50,  1)) 
    table.insertRow(Array(5, 60, 900, 0))
    table.insertRow(Array(6, 18, 850, 1)) // Young but high score
    
    table.flush()
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // 1. BASIC COMPOUND TESTS
  // -------------------------------------------------------------------------

  test("1. Compound: Should handle basic AND logic") {
    val sql = "SELECT id FROM users WHERE age < 40 AND score >= 200"
    val res = SQLHandler.execute(sql)
    
    // User 2 (30, 500), User 3 (25, 200), User 6 (18, 850)
    assert(res.contains("   2"), "Missed User 2")
    assert(res.contains("   3"), "Missed User 3")
    assert(res.contains("   6"), "Missed User 6")
    assert(!res.contains("   1"), "Incorrectly included User 1")
  }

  test("2. Compound: Should handle overlapping OR logic without duplicating rows") {
    val sql = "SELECT id FROM users WHERE age < 25 OR score < 300"
    val res = SQLHandler.execute(sql)
    
    // age < 25: Users 1, 6
    // score < 300: Users 1, 3, 4
    // Union should be: 1, 3, 4, 6 (User 1 matches BOTH, must not appear twice)
    
    val matches = "1".r.findAllIn(res).length
    assert(matches == 1, "Set Union failed to deduplicate User 1")
    assert(res.contains("   6"), "Missed OR condition for User 6")
    assert(res.contains("   4"), "Missed OR condition for User 4")
  }

  // -------------------------------------------------------------------------
  // 2. CHAINING & EDGE CASES
  // -------------------------------------------------------------------------

  test("3. Compound: Should handle multiple chained ANDs (A AND B AND C)") {
    val sql = "SELECT id FROM users WHERE age < 50 AND score > 100 AND active = 1"
    val res = SQLHandler.execute(sql)
    
    // Age < 50, Score > 100, Active = 1
    // Matches: User 2 (30, 500, 1), User 6 (18, 850, 1)
    // User 3 fails 'active', User 1 fails 'score'
    assert(res.contains("   2"), "Missed User 2")
    assert(res.contains("   6"), "Missed User 6")
    assert(!res.contains("   3"), "Failed to enforce third AND condition")
  }

  test("4. Compound: Should handle impossible conditions cleanly (Empty Result)") {
    val sql = "SELECT id FROM users WHERE age > 50 AND age < 20"
    val res = SQLHandler.execute(sql)
    
    assert(!res.contains("Error"), "Impossible condition threw an error")
    assert(!res.contains("   1"), "Impossible condition returned rows")
  }

  // -------------------------------------------------------------------------
  // 3. PRECEDENCE & AST PARENTHESES
  // -------------------------------------------------------------------------

  test("5. Compound: Should respect standard SQL precedence (AND before OR)") {
    // SQL Rule: A OR B AND C is evaluated as A OR (B AND C)
    val sql = "SELECT id FROM users WHERE score > 800 OR age < 30 AND active = 0"
    val res = SQLHandler.execute(sql)
    
    // B AND C (age < 30 AND active = 0) -> User 3
    // A (score > 800) -> User 5, User 6
    // Expected Union: 3, 5, 6
    assert(res.contains("   3"), "Missed User 3 from right-side AND")
    assert(res.contains("   5"), "Missed User 5 from left-side OR")
    assert(res.contains("   6"), "Missed User 6 from left-side OR")
    assert(!res.contains("   1"), "Incorrectly evaluated precedence")
  }

  test("6. Compound: Should support explicit Parentheses for logic grouping") {
    // Override precedence: (A OR B) AND C
    val sql = "SELECT id FROM users WHERE (score > 800 OR age < 30) AND active = 0"
    val res = SQLHandler.execute(sql)
    
    // (score > 800 OR age < 30) -> Users 1, 2, 3, 5, 6
    // AND active = 0 -> Users 3, 5
    // Expected Intersection: 3, 5
    assert(res.contains("   3"), "Missed User 3")
    assert(res.contains("   5"), "Missed User 5")
    assert(!res.contains("   6"), "Failed to apply AND condition outside parentheses to User 6")
  }
}