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
import org.awandb.core.query._
import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class MaterializationSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = new File("data/materialize_test").getAbsolutePath

  def cleanUp(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
    dir.mkdirs()
  }

  "AwanDB Late Materialization" should "fetch non-key columns after a Join" in {
    cleanUp()
    
    // SETUP: Orders Table with an "Amount" column (Column 2)
    // 0: OID, 1: UID_FK, 2: AMOUNT
    val orders = new AwanTable("orders_mat", 1000, TEST_DIR)
    orders.addColumn("oid"); orders.addColumn("uid_fk"); orders.addColumn("amount")
    
    orders.insertRow(Array(100, 1, 500))  // Row 0: User 1 spent 500
    orders.insertRow(Array(101, 1, 300))  // Row 1: User 1 spent 300
    orders.insertRow(Array(102, 2, 900))  // Row 2: User 2 spent 900
    orders.insertRow(Array(103, 99, 100)) // Row 3: User 99 (No Match)
    orders.flush()
    
    // SETUP: Users Table
    val users = new AwanTable("users_mat", 1000, TEST_DIR)
    users.addColumn("uid"); users.addColumn("age")
    users.insertRow(Array(1, 25))
    users.insertRow(Array(2, 30))
    users.flush()
    
    // BUILD PLAN
    // 1. Build Hash on Users
    val usersScan = new TableScanOperator(users.blockManager, 0, 1)
    val usersAgg = new HashAggOperator(usersScan)
    val buildOp = new HashJoinBuildOperator(usersAgg)
    
    // 2. Probe Orders (Key = UID_FK)
    // Note: We only scan Col 1 (UID_FK). We do NOT read Col 2 (Amount) yet!
    val ordersScan = new TableScanOperator(orders.blockManager, 1, 0)
    val joinOp = new HashJoinProbeOperator(ordersScan, buildOp)
    
    // 3. Materialize Amount (Col 2)
    // Takes the matching Row IDs from Join and fetches Col 2 from Orders.
    val matOp = new MaterializeOperator(joinOp, colIdx = 2)
    
    // EXECUTE
    matOp.open()
    val batch = matOp.next()
    
    // VERIFY
    // Matched Rows: 0 (500), 1 (300), 2 (900).
    // Result Batch: Keys=UID, Values=AMOUNT (Materialized)
    batch.count shouldBe 3
    
    val amounts = new Array[Long](3) // Materialized into valuesPtr
    NativeBridge.copyToScalaLong(batch.valuesPtr, amounts, 3)
    
    println(s"Materialized Amounts: ${amounts.mkString(", ")}")
    
    amounts should contain (500L)
    amounts should contain (300L)
    amounts should contain (900L)
    
    matOp.close()
    users.close(); orders.close()
  }
}