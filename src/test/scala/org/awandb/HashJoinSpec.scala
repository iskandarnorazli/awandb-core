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

class HashJoinSpec extends AnyFlatSpec with Matchers {

  val BASE_TEST_DIR = new File("data/hash_join_extensive").getAbsolutePath

  def cleanUp(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
    dir.mkdirs()
  }

  // --- TEST 1: FUNCTIONAL CORRECTNESS (Small Scale) ---
  "AwanDB Join Operator" should "perform an INNER JOIN correctly (Basic)" in {
    val usersDir = s"$BASE_TEST_DIR/basic/users"
    val ordersDir = s"$BASE_TEST_DIR/basic/orders"
    cleanUp(usersDir); cleanUp(ordersDir)
    
    // [FIX] Isolate Data Directories
    val users = new AwanTable("users_basic", 1000, usersDir)
    users.addColumn("uid"); users.addColumn("age")
    users.insertRow(Array(1, 25)); users.insertRow(Array(2, 30)); users.insertRow(Array(3, 40))
    users.flush()
    
    val orders = new AwanTable("orders_basic", 1000, ordersDir)
    orders.addColumn("oid"); orders.addColumn("uid_fk")
    orders.insertRow(Array(100, 1)); orders.insertRow(Array(101, 1)); orders.insertRow(Array(102, 2)); orders.insertRow(Array(103, 99))
    orders.flush()
    
    val usersScan = new TableScanOperator(users.blockManager, 0, 1) 
    val usersAgg = new HashAggOperator(usersScan) 
    val buildOp = new HashJoinBuildOperator(usersAgg)
    val ordersScan = new TableScanOperator(orders.blockManager, 1, 0)
    val joinOp = new HashJoinProbeOperator(ordersScan, buildOp)
    
    joinOp.open()
    val batch = joinOp.next()
    
    batch.count shouldBe 3
    val keys = new Array[Int](3)
    val vals = new Array[Long](3)
    NativeBridge.copyToScala(batch.keysPtr, keys, 3)
    NativeBridge.copyToScalaLong(batch.valuesPtr, vals, 3)
    
    val results = keys.zip(vals)
    results should contain (1, 25L)
    results should contain (2, 30L)
    
    joinOp.close(); users.close(); orders.close()
  }

  // --- TEST 2: HIGH CARDINALITY SCALING & INTEGRITY ---
  it should "scale to large datasets (10k Users, 100k Orders)" in {
    val usersDir = s"$BASE_TEST_DIR/scale/users"
    val ordersDir = s"$BASE_TEST_DIR/scale/orders"
    cleanUp(usersDir); cleanUp(ordersDir)
    
    val numUsers = 10000
    val numOrders = 100000
    val rnd = new Random(42)
    
    // 1. Generate Users (Build Side)
    // [FIX] Use isolated directory
    val users = new AwanTable("users_scale", 200000, usersDir)
    users.addColumn("uid"); users.addColumn("age")
    for (i <- 0 until numUsers) users.insertRow(Array(i, rnd.nextInt(80) + 18))
    users.flush()
    
    // 2. Generate Orders (Probe Side)
    // [FIX] Use isolated directory
    val orders = new AwanTable("orders_scale", 200000, ordersDir)
    orders.addColumn("oid"); orders.addColumn("uid_fk")
    
    var expectedMatches = 0
    for (i <- 0 until numOrders) {
      val uid = if (rnd.nextBoolean()) rnd.nextInt(numUsers) else numUsers + rnd.nextInt(1000)
      if (uid < numUsers) expectedMatches += 1
      orders.insertRow(Array(i, uid))
    }
    orders.flush()
    
    println(s"\n=== [SETUP] Generated $numUsers Users, $numOrders Orders. Expected Matches: $expectedMatches ===")
    
    // --- AUDIT 1: CHECK ORDERS TABLE ---
    println("--- [AUDIT] Scanning Orders Table ---")
    val ordersAuditScan = new TableScanOperator(orders.blockManager, 1, 0)
    ordersAuditScan.open()
    var orderCount = 0
    var orderBatch = ordersAuditScan.next()
    while (orderBatch != null) {
        orderCount += orderBatch.count
        orderBatch = ordersAuditScan.next()
    }
    ordersAuditScan.close()
    println(s"Orders Audit: Found $orderCount rows (Expected $numOrders).")
    
    // This MUST pass now that directories are isolated
    orderCount shouldBe numOrders

    // 3. Execute Join
    val usersScan = new TableScanOperator(users.blockManager, 0, 1)
    val usersAgg = new HashAggOperator(usersScan) // Cast payload
    val buildOp = new HashJoinBuildOperator(usersAgg)
    
    val ordersScan = new TableScanOperator(orders.blockManager, 1, 0)
    val joinOp = new HashJoinProbeOperator(ordersScan, buildOp)
    
    joinOp.open()
    
    var totalJoined = 0
    var batch = joinOp.next()
    var batchCount = 0
    var invalidMatches = 0
    
    val t0 = System.nanoTime()
    while (batch != null) {
      val keys = new Array[Int](batch.count)
      NativeBridge.copyToScala(batch.keysPtr, keys, batch.count)
      
      keys.foreach { k =>
          if (k >= numUsers) {
              invalidMatches += 1
              if (invalidMatches <= 5) println(s"[ERROR] Ghost Match Found! Key=$k matched")
          }
      }

      totalJoined += batch.count
      batchCount += 1
      batch = joinOp.next()
    }
    val t1 = System.nanoTime()
    
    println(s"=== [RESULT] Joined $totalJoined rows in ${(t1-t0)/1e6} ms ($batchCount batches) ===")
    
    if (invalidMatches > 0) {
        fail(s"INTEGRITY FAIL: Found $invalidMatches matches for keys that do not exist in the Users table!")
    }
    
    totalJoined shouldBe expectedMatches
    
    joinOp.close()
    users.close(); orders.close()
  }
}