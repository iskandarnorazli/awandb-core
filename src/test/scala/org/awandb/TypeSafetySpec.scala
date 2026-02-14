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
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import java.io.File

class TypeSafetySpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "strict_products"
  val dataDir = "data_typesafe_test"
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

    table = new AwanTable(tableName, 1000, dataDir)
    table.addColumn("id")              // Int
    table.addColumn("name", true)      // String (isString = true)
    table.addColumn("price")           // Int
    
    SQLHandler.register(tableName, table)
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // TYPE SAFETY & SCHEMA TESTS
  // -------------------------------------------------------------------------

  test("1. Schema: Should reject INSERT with missing columns") {
    // Table expects 3 columns (id, name, price). We provide 2.
    val res = SQLHandler.execute(s"INSERT INTO $tableName VALUES (1, 'Apple')")
    
    assert(res.contains("Column mismatch") || res.contains("Error"), 
      s"Engine allowed an undersized row! Response: $res")
  }

  test("2. Schema: Should reject INSERT with too many columns") {
    // Table expects 3 columns. We provide 4.
    val res = SQLHandler.execute(s"INSERT INTO $tableName VALUES (2, 'Banana', 50, 999)")
    
    assert(res.contains("Column mismatch") || res.contains("Error"), 
      s"Engine allowed an oversized row! Response: $res")
  }

  test("3. Type Safety: Should reject inserting a String into an Int column") {
    // Column 3 ('price') is an Int. We provide a String ('Expensive').
    val res = SQLHandler.execute(s"INSERT INTO $tableName VALUES (3, 'Cherry', 'Expensive')")
    
    assert(res.contains("expects Int, but got String") || res.contains("Error"), 
      s"Engine tried to cast a String to an Int! Response: $res")
  }
}