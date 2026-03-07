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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.{SQLHandler, SQLResult}
import org.apache.arrow.flight.{CallHeaders, FlightCallHeaders}

// Correctly import the middleware!
import org.awandb.server.middleware.{FormatMiddleware, FormatMiddlewareFactory}

class AwanEgressPipelineSpec extends AnyFlatSpec with Matchers {

  "SQLHandler" should "return structured columnar data for Integers" in {
    val tableName = "egress_test_int"
    SQLHandler.execute(s"DROP TABLE $tableName") 
    SQLHandler.execute(s"CREATE TABLE $tableName (id INT, price INT)")
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 100)")
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (2, 200)")

    val result = SQLHandler.execute(s"SELECT id, price FROM $tableName WHERE price > 50")

    result.isError shouldBe false
    result.schema.length shouldBe 2
    result.columnarData.length shouldBe 2
    
    val idColumn = result.columnarData(0).asInstanceOf[Array[Int]]
    val priceColumn = result.columnarData(1).asInstanceOf[Array[Int]]
    
    idColumn should contain theSameElementsInOrderAs Seq(1, 2)
    priceColumn should contain theSameElementsInOrderAs Seq(100, 200)
  }

  it should "return structured columnar data for Strings" in {
    val tableName = "egress_test_str"
    SQLHandler.execute(s"DROP TABLE $tableName") 
    SQLHandler.execute(s"CREATE TABLE $tableName (id INT, name STRING)")
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 'Apple')")
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (2, 'Banana')")

    val result = SQLHandler.execute(s"SELECT name FROM $tableName ORDER BY id")

    result.isError shouldBe false
    result.schema(0) shouldBe ("name", "STRING")
    
    val nameColumn = result.columnarData(0).asInstanceOf[Array[String]]
    nameColumn should contain theSameElementsInOrderAs Seq("Apple", "Banana")
  }

  it should "return an empty array but a valid schema when no rows match" in {
    val tableName = "egress_test_empty"
    SQLHandler.execute(s"DROP TABLE $tableName") 
    SQLHandler.execute(s"CREATE TABLE $tableName (id INT, price INT)")
    SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 100)")

    // Query looking for a price that doesn't exist
    val result = SQLHandler.execute(s"SELECT id, price FROM $tableName WHERE price = 999")

    result.isError shouldBe false
    result.affectedRows shouldBe 0
    
    // Schema MUST still exist for Arrow to initialize the VectorSchemaRoot
    result.schema.length shouldBe 2
    result.schema(0) shouldBe ("id", "INT")

    // Columnar arrays must be empty, not null
    val idColumn = result.columnarData(0).asInstanceOf[Array[Int]]
    idColumn.length shouldBe 0
  }

  "FormatMiddleware" should "extract x-awan-format header into the CallContext" in {
    val headers: CallHeaders = new FlightCallHeaders()
    headers.insert("x-awan-format", "arrow")

    val factory = new FormatMiddlewareFactory()
    val middleware = factory.onCallStarted(null, headers, null)

    middleware.asInstanceOf[FormatMiddleware].getFormat() shouldBe "arrow"
  }

  it should "default to 'string' format if the header is missing" in {
    val emptyHeaders: CallHeaders = new FlightCallHeaders()

    val factory = new FormatMiddlewareFactory()
    val middleware = factory.onCallStarted(null, emptyHeaders, null)

    // Should safely fallback to standard OLTP payload
    middleware.asInstanceOf[FormatMiddleware].getFormat() shouldBe "string"
  }
}