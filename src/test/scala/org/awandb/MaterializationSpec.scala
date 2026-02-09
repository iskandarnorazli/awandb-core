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
import scala.util.Random

class MaterializationSpec extends AnyFlatSpec with Matchers {

  val BASE_TEST_DIR = new File("data/materialize_test").getAbsolutePath

  def cleanUp(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
    dir.mkdirs()
  }

  "AwanDB Late Materialization" should "fetch non-key columns correctly (Small Scale)" in {
    val dir = s"$BASE_TEST_DIR/small"
    cleanUp(dir)
    
    // SETUP: Orders (0:OID, 1:UID, 2:AMOUNT)
    val orders = new AwanTable("orders", 1000, dir)
    orders.addColumn("oid"); orders.addColumn("uid"); orders.addColumn("amount")
    orders.insertRow(Array(100, 1, 500))
    orders.insertRow(Array(101, 1, 300))
    orders.insertRow(Array(102, 2, 900))
    orders.insertRow(Array(103, 99, 100))
    orders.flush()
    
    val users = new AwanTable("users", 1000, dir)
    users.addColumn("uid"); users.addColumn("age")
    users.insertRow(Array(1, 25)); users.insertRow(Array(2, 30))
    users.flush()
    
    // PLAN
    val usersScan = new TableScanOperator(users.blockManager, 0, 1)
    val usersAgg = new HashAggOperator(usersScan)
    val buildOp = new HashJoinBuildOperator(usersAgg)
    
    val ordersScan = new TableScanOperator(orders.blockManager, 1, 0)
    val joinOp = new HashJoinProbeOperator(ordersScan, buildOp)
    val matOp = new MaterializeOperator(joinOp, colIdx = 2) // Fetch Amount
    
    matOp.open()
    val batch = matOp.next()
    
    batch.count shouldBe 3
    val amounts = new Array[Long](3)
    NativeBridge.copyToScalaLong(batch.valuesPtr, amounts, 3)
    
    amounts should contain (500L)
    amounts should contain (300L)
    amounts should contain (900L)
    
    matOp.close(); users.close(); orders.close()
  }

  it should "scale to 50k rows (Stress Test Gather Kernel)" in {
    val usersDir = s"$BASE_TEST_DIR/scale/users"
    val ordersDir = s"$BASE_TEST_DIR/scale/orders"
    cleanUp(usersDir); cleanUp(ordersDir)
    
    val count = 50000
    
    // 1. Users (Build Side)
    val users = new AwanTable("users_scale", 100000, usersDir)
    users.addColumn("uid"); users.addColumn("val")
    for (i <- 0 until 1000) users.insertRow(Array(i, 1)) // 1000 Users
    users.flush()
    
    // 2. Orders (Probe Side)
    val orders = new AwanTable("orders_scale", 100000, ordersDir)
    orders.addColumn("oid"); orders.addColumn("uid_fk"); orders.addColumn("price")
    
    var expectedSum: Long = 0
    val rnd = new Random(42)
    
    for (i <- 0 until count) {
      val uid = rnd.nextInt(1000) // Always matches
      val price = rnd.nextInt(100)
      orders.insertRow(Array(i, uid, price))
      expectedSum += price
    }
    orders.flush()
    
    println(s"[SETUP] Generated $count Orders. Expected Sum: $expectedSum")
    
    // 3. Pipeline
    val usersScan = new TableScanOperator(users.blockManager, 0, 1)
    val buildOp = new HashJoinBuildOperator(new HashAggOperator(usersScan))
    
    val ordersScan = new TableScanOperator(orders.blockManager, 1, 0)
    val joinOp = new HashJoinProbeOperator(ordersScan, buildOp)
    
    // MATERIALIZE 'price' (Column 2)
    val matOp = new MaterializeOperator(joinOp, colIdx = 2)
    
    matOp.open()
    var totalSum: Long = 0
    var rowsProcessed = 0
    var batch = matOp.next()
    
    val t0 = System.nanoTime()
    while (batch != null) {
      val prices = new Array[Long](batch.count)
      NativeBridge.copyToScalaLong(batch.valuesPtr, prices, batch.count)
      
      var i = 0
      while(i < batch.count) {
        totalSum += prices(i)
        i += 1
      }
      
      rowsProcessed += batch.count
      batch = matOp.next()
    }
    val t1 = System.nanoTime()
    
    println(s"[RESULT] Materialized $rowsProcessed rows in ${(t1-t0)/1e6} ms. Sum: $totalSum")
    
    rowsProcessed shouldBe count
    totalSum shouldBe expectedSum
    
    matOp.close(); users.close(); orders.close()
  }
}