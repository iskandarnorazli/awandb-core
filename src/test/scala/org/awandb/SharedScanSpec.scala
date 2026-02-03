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
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.Executors

class SharedScanSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = "data/shared_scan_test"
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  "AwanDB Query Engine" should "execute multiple queries in a single fused batch" in {
    // Cleanup
    val dir = new File(TEST_DIR)
    if (dir.exists()) { dir.listFiles().foreach(_.delete()); dir.delete() }
    new File(TEST_DIR).mkdirs()

    // FIX: Added name "shared_scan" to constructor
    val table = new AwanTable("shared_scan", 10000, TEST_DIR)
    
    // FIX: Must define schema before inserting
    table.addColumn("val")

    // 1. Insert Data (0 to 999)
    for (i <- 0 until 1000) {
      // FIX: Use engineManager.submitInsert for async ingestion
      table.engineManager.submitInsert(i)
    }
    
    // Wait for data to be ingested (1 sec is plenty for 1000 rows)
    Thread.sleep(1000) 

    println("\n--- Testing Shared Scan (Query Fusion) ---")
    
    // 2. Simulate 3 Concurrent Users with DIFFERENT queries
    // The EngineManager will fuse these into one AVX scan pass if they arrive close together.
    
    // User A: > 800  (Expect 801..999 = 199 items)
    val f1 = table.engineManager.submitQuery(800)
    
    // User B: > 500  (Expect 501..999 = 499 items)
    val f2 = table.engineManager.submitQuery(500)
    
    // User C: > 100  (Expect 101..999 = 899 items)
    val f3 = table.engineManager.submitQuery(100)

    // 3. Wait for results
    val results = Await.result(Future.sequence(Seq(f1, f2, f3)), 5.seconds)
    
    val countA = results(0)
    val countB = results(1)
    val countC = results(2)

    println(f"Query A (>800): $countA match")
    println(f"Query B (>500): $countB match")
    println(f"Query C (>100): $countC match")

    // 4. Verification
    countA shouldBe 199
    countB shouldBe 499
    countC shouldBe 899

    table.close()
  }
}