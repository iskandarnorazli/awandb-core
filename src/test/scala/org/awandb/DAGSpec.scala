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

import org.awandb.core.engine.AwanTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random

class DAGSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = new File("data/dag_spec_debug").getAbsolutePath

  def cleanUp(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  // --- TEST 1: BASELINE (The "Happy Path") ---
  "AwanDB Operator DAG" should "execute SELECT SUM(revenue) GROUP BY city (Int)" in {
    cleanUp()
    val table = new AwanTable("sales_simple", 50000, TEST_DIR) 
    table.addColumn("city_id") 
    table.addColumn("revenue")
    
    val rows = 20000
    for (i <- 0 until rows) {
      if (i % 2 == 0) table.insertRow(Array(1, 100)) 
      else table.insertRow(Array(2, 200))
    }
    table.flush() 
    
    val result = table.executeGroupBy("city_id", "revenue")
    
    val expected1 = 1_000_000L
    val actual1 = result.getOrElse(1, 0L)
    withClue(s"City 1: Expected $expected1, Actual $actual1 (Diff: ${actual1 - expected1})") {
      actual1 shouldBe expected1
    }

    val expected2 = 2_000_000L
    val actual2 = result.getOrElse(2, 0L)
    withClue(s"City 2: Expected $expected2, Actual $actual2 (Diff: ${actual2 - expected2})") {
      actual2 shouldBe expected2
    }
    
    table.close()
  }

  // --- TEST 2: DICTIONARY INTEGRATION (DEBUG MODE) ---
  it should "execute GROUP BY on a Dictionary-Encoded String Column" in {
    cleanUp()
    val table = new AwanTable("sales_dict", 50000, TEST_DIR)
    
    table.addColumn("city_name", isString = true, useDictionary = true)
    table.addColumn("revenue")
    
    val cities = Array("Kuala Lumpur", "Penang", "Johor Bahru", "Kuching")
    val rnd = new Random(42)
    val rows = 10000
    
    val expected = scala.collection.mutable.Map[String, Long]().withDefaultValue(0L)
    
    // 1. Generate Data
    for (_ <- 0 until rows) {
      val city = cities(rnd.nextInt(cities.length))
      val rev = 100
      table.insertRow(Array(city, rev))
      expected(city) += rev
    }
    table.flush()
    
    // 2. DEBUG: Print Dictionary Mappings
    println("\n=== [DEBUG] Dictionary Mapping ===")
    val col = table.columns("city_name")
    
    cities.foreach { city =>
      val id = col.getDictId(city)
      println(f"  String: $city%-15s -> ID: $id")
    }
    println("==================================")

    // 3. EXECUTE ENGINE
    val resultIDs = table.executeGroupBy("city_name", "revenue")
    
    // 4. DEBUG: Print Results vs Expected
    println("\n=== [DEBUG] Aggregation Results ===")
    var mismatchFound = false 
    
    cities.foreach { city =>
      val id = col.getDictId(city)
      val actualSum = resultIDs.getOrElse(id, 0L)
      val expectedSum = expected(city)
      val diff = actualSum - expectedSum
      
      val status = if (diff == 0) "OK" else "FAIL"
      if (diff != 0) mismatchFound = true
      
      // [FIX] Removed single quotes around city to fix f-interpolator error
      println(f"  City: $city%-15s (ID $id%2d) | Expected: $expectedSum%8d | Actual: $actualSum%8d | Diff: $diff%+d [$status]")
    }
    println("==================================\n")
    
    if (mismatchFound) fail("Aggregation Mismatch! Check logs above.")
    
    table.close()
  }

  // --- TEST 3: HIGH CARDINALITY (DEBUG MODE) ---
  it should "handle High Cardinality Aggregation" in {
    cleanUp()
    val table = new AwanTable("sales_stress", 200000, TEST_DIR)
    table.addColumn("user_id")
    table.addColumn("clicks")
    
    val uniqueUsers = 10_000
    val totalRows = 100_000
    val rnd = new Random(123)
    
    var rowsInserted = 0
    
    // Phase 1: Ensure EVERY user ID is inserted at least once
    for (user <- 0 until uniqueUsers) {
      table.insertRow(Array(user, 1))
      rowsInserted += 1
    }
    
    // Phase 2: Fill the rest
    for (_ <- rowsInserted until totalRows) {
      val user = rnd.nextInt(uniqueUsers)
      table.insertRow(Array(user, 1)) 
    }
    
    table.flush()
    
    val t0 = System.nanoTime()
    val result = table.executeGroupBy("user_id", "clicks")
    val t1 = System.nanoTime()
    
    // [FIX] Extract calculation to variable to fix f-interpolator error
    val durationMs = (t1 - t0) / 1e6
    println(f"High Cardinality Aggregation Time: $durationMs%.2f ms")
    
    val totalCount = result.values.sum
    
    println("\n=== [DEBUG] High Cardinality Stats ===")
    println(s"  Rows Inserted: $totalRows")
    println(s"  Unique Users:  $uniqueUsers")
    println(s"  Result Groups: ${result.size}")
    println(s"  Total Count:   $totalCount")
    
    // Diagnosis Logic
    if (result.size != uniqueUsers) {
        println(s"  [ERROR] Missing Groups: ${uniqueUsers - result.size}")
        // Print first 5 missing keys if any
        val returnedKeys = result.keySet
        val missing = (0 until uniqueUsers).filterNot(returnedKeys.contains).take(10)
        println(s"  [SAMPLE] Missing User IDs: $missing ...")
    }
    
    if (totalCount != totalRows) {
        println(s"  [ERROR] Missing/Extra Rows: ${totalCount - totalRows}")
    }
    println("======================================\n")

    result.size shouldBe uniqueUsers
    result.values.sum shouldBe totalRows
    
    table.close()
  }
  
  // --- TEST 4: EMPTY TABLE (The "Edge Case" Test) ---
  it should "return empty map for empty table" in {
    cleanUp()
    val table = new AwanTable("empty_test", 100, TEST_DIR)
    table.addColumn("a")
    table.addColumn("b")
    table.flush()
    
    val result = table.executeGroupBy("a", "b")
    result shouldBe empty
    
    table.close()
  }
}