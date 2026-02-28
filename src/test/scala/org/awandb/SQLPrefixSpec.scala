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

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.jni.NativeBridge
import org.awandb.core.sql.SQLHandler

class SQLPrefixSpec extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    // Initialize the native C++ bridge before running tests
    NativeBridge.init()

    // 1. Setup Test Tables
    SQLHandler.execute("CREATE TABLE users (user_id INT, age INT, score INT)")
    SQLHandler.execute("CREATE TABLE orders (order_id INT, user_id INT, amount INT)")

    // 2. Seed Data
    SQLHandler.execute("INSERT INTO users (user_id, age, score) VALUES (1, 25, 100)")
    SQLHandler.execute("INSERT INTO users (user_id, age, score) VALUES (2, 30, 200)")
    
    SQLHandler.execute("INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 50)")
    SQLHandler.execute("INSERT INTO orders (order_id, user_id, amount) VALUES (102, 1, 150)")
  }

  override def afterAll(): Unit = {
    // Teardown
    SQLHandler.execute("DROP TABLE users")
    SQLHandler.execute("DROP TABLE orders")
  }

  test("1. SELECT Projection with Table Prefixes") {
    val result = SQLHandler.execute("SELECT users.user_id, users.score FROM users")
    
    assert(!result.isError, s"Query failed: ${result.message}")
    assert(result.message.contains("Found Rows (users.user_id | users.score)"))
    assert(result.affectedRows == 2L)
  }

  test("2. WHERE Clause with Table Prefixes") {
    val result = SQLHandler.execute("SELECT user_id FROM users WHERE users.age > 26")
    
    assert(!result.isError, s"Query failed: ${result.message}")
    // Should only find user 2 (age 30)
    assert(result.message.contains("2"))
    assert(result.affectedRows == 1L)
  }

  test("3. Aggregation (SUM, MAX) with Table Prefixes") {
    val result = SQLHandler.execute("SELECT SUM(users.score), MAX(users.age) FROM users")
    
    assert(!result.isError, s"Query failed: ${result.message}")
    assert(result.message.contains("SUM(users.score) | MAX(users.age)"))
    // Sum of scores: 100 + 200 = 300, Max age = 30
    assert(result.message.contains("300 | 30"))
  }

  test("4. ORDER BY with Table Prefixes") {
    val result = SQLHandler.execute("SELECT users.user_id FROM users ORDER BY users.score DESC LIMIT 1")
    
    assert(!result.isError, s"Query failed: ${result.message}")
    // User 2 has the highest score (200)
    assert(result.message.contains("2"))
  }

  test("5. UPDATE with Table Prefixes in WHERE clause") {
    val result = SQLHandler.execute("UPDATE users SET score = 999 WHERE users.user_id = 1")
    
    assert(!result.isError, s"Query failed: ${result.message}")
    assert(result.affectedRows == 1L)

    // Verify the update
    val check = SQLHandler.execute("SELECT score FROM users WHERE user_id = 1")
    assert(check.message.contains("999"))
  }

  test("6. JOIN query using extensive prefixes") {
    val query = """
      SELECT users.user_id, SUM(orders.amount)
      FROM orders
      JOIN users ON orders.user_id = users.user_id
      WHERE users.user_id = 1
      GROUP BY users.user_id
    """
    val result = SQLHandler.execute(query)
    
    assert(!result.isError, s"Query failed: ${result.message}")
    // Join matches user 1 -> order 101 (50) and 102 (150). Sum should be 200.
    assert(result.message.contains("200"))
  }
}