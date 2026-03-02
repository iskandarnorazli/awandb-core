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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import java.io.File

class AggregationAdvancedSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val testDir = "target/data_sql_agg"
  var aggTable: AwanTable = _

  override def beforeEach(): Unit = {
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    else dir.mkdirs()
    
    aggTable = new AwanTable("agg_test", 1000, testDir)
    aggTable.addColumn("id")
    aggTable.addColumn("volume")
    SQLHandler.register("agg_test", aggTable)

    // Insert 10 rows: ID and Volume = 1 to 10
    for (i <- 1 to 10) {
      aggTable.insertRow(Array(i, i))
    }
  }

  override def afterEach(): Unit = {
    aggTable.close()
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
  }

  test("Fast-Path Full Table Aggregation (Zero Allocation)") {
    // Expected: Count=10, Sum=55, Min=1, Max=10, Avg=5.5
    val sql = "SELECT COUNT(*), SUM(volume), MIN(volume), MAX(volume), AVG(volume) FROM agg_test"
    val result = SQLHandler.execute(sql)

    result.isError shouldBe false
    result.message should include("10 | 55 | 1 | 10 | 5.5")
  }

  test("Fast-Path Filtered Aggregation (Zero Allocation)") {
    // Filter: id > 5 (Matches IDs 6, 7, 8, 9, 10)
    // Expected: Count=5, Sum=40, Min=6, Max=10, Avg=8.0
    val sql = "SELECT COUNT(*), SUM(volume), MIN(volume), MAX(volume), AVG(volume) FROM agg_test WHERE id > 5"
    val result = SQLHandler.execute(sql)

    result.isError shouldBe false
    result.message should include("5 | 40 | 6 | 10 | 8.0")
  }
}