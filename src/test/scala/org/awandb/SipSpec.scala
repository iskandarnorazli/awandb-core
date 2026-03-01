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

package org.awandb.core.query

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge

class SipSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {
  
  var factTable: AwanTable = _
  var dimTable: AwanTable = _

  override def beforeEach(): Unit = {
    val dir1 = new java.io.File("target/data_fact")
    if (dir1.exists()) dir1.listFiles().foreach(_.delete())
    else dir1.mkdirs()

    val dir2 = new java.io.File("target/data_dim")
    if (dir2.exists()) dir2.listFiles().foreach(_.delete())
    else dir2.mkdirs()

    factTable = new AwanTable("fact", 10000, "target/data_fact")
    factTable.addColumn("dim_id", isString = false)
    factTable.addColumn("amount", isString = false)

    dimTable = new AwanTable("dim", 1000, "target/data_dim")
    dimTable.addColumn("id", isString = false)
    dimTable.addColumn("name_hash", isString = false) 
  }

  override def afterEach(): Unit = {
    factTable.close()
    dimTable.close()
  }

  test("SIP should filter rows before Hash Probe phase (Partial Match)") {
    dimTable.insertRow(Array(10, 100))
    dimTable.insertRow(Array(20, 200))
    dimTable.insertRow(Array(30, 300))
    dimTable.flush()

    for (i <- 1 to 1000) factTable.insertRow(Array(i, i * 5))
    factTable.flush()

    val buildOp = new HashJoinBuildOperator(new TableScanOperator(dimTable.blockManager, 0, 1))
    buildOp.open()
    buildOp.next() 

    val sipFilter = new SipFilterOperator(new TableScanOperator(factTable.blockManager, 0, 1), buildOp.getCuckooPtr())
    val probeOp = new HashJoinProbeOperator(sipFilter, buildOp)
    probeOp.open()

    var totalProbed = 0
    var batch = probeOp.next()
    while (batch != null && batch.count > 0) {
      totalProbed += batch.count
      batch = probeOp.next()
    }

    totalProbed shouldBe 3 // Only 10, 20, 30 survive
    probeOp.close()
  }

  test("SIP should handle 100% rejection gracefully (Zero Matches)") {
    dimTable.insertRow(Array(9999, 100)) // No matching facts
    dimTable.flush()

    for (i <- 1 to 1000) factTable.insertRow(Array(i, i * 5))
    factTable.flush()

    val buildOp = new HashJoinBuildOperator(new TableScanOperator(dimTable.blockManager, 0, 1))
    buildOp.open()
    buildOp.next()

    val sipFilter = new SipFilterOperator(new TableScanOperator(factTable.blockManager, 0, 1), buildOp.getCuckooPtr())
    val probeOp = new HashJoinProbeOperator(sipFilter, buildOp)
    probeOp.open()

    val batch = probeOp.next()
    
    // Batch should be strictly null, meaning the SIP filter choked off all IO before the probe.
    batch shouldBe null 
    probeOp.close()
  }

  test("SIP should handle 100% match rate gracefully (All Matches)") {
    for (i <- 1 to 100) dimTable.insertRow(Array(i, i * 10))
    dimTable.flush()

    for (i <- 1 to 100) factTable.insertRow(Array(i, i * 5))
    factTable.flush()

    val buildOp = new HashJoinBuildOperator(new TableScanOperator(dimTable.blockManager, 0, 1))
    buildOp.open()
    buildOp.next()

    val sipFilter = new SipFilterOperator(new TableScanOperator(factTable.blockManager, 0, 1), buildOp.getCuckooPtr())
    val probeOp = new HashJoinProbeOperator(sipFilter, buildOp)
    probeOp.open()

    var totalProbed = 0
    var batch = probeOp.next()
    while (batch != null && batch.count > 0) {
      totalProbed += batch.count
      batch = probeOp.next()
    }

    totalProbed shouldBe 100 // Everything passes through safely
    probeOp.close()
  }

  test("Late Materialization should fetch correct uncorrupted payload after SIP compression") {
    // Only IDs 50, 51, and 52 exist in the Dimension table
    dimTable.insertRow(Array(50, 100))
    dimTable.insertRow(Array(51, 200))
    dimTable.insertRow(Array(52, 300))
    dimTable.flush()

    // Fact table contains 1 to 100. Payload amount is exactly ID * 10
    for (i <- 1 to 100) {
      factTable.insertRow(Array(i, i * 10))
    }
    factTable.flush()

    val buildOp = new HashJoinBuildOperator(new TableScanOperator(dimTable.blockManager, 0, 1))
    buildOp.open()
    buildOp.next()

    val sipFilter = new SipFilterOperator(new TableScanOperator(factTable.blockManager, 0, 1), buildOp.getCuckooPtr())
    val probeOp = new HashJoinProbeOperator(sipFilter, buildOp)
    
    // Wire up Late Materialization! 
    // We want to fetch Column 1 ("amount") from the Fact Table, BUT only for the rows that survived.
    val materializeOp = new MaterializeOperator(probeOp, colIdx = 1)
    
    materializeOp.open()

    val batch = materializeOp.next()
    batch should not be null
    batch.count shouldBe 3 // 50, 51, 52

    // Verify the materialized payload values.
    // [FIX] MaterializeOperator widened the 32-bit disk Ints to 64-bit Longs for the join payload.
    // So we must allocate space for 6 Ints (24 bytes) to safely read 3 Longs.
    val amounts = new Array[Int](6) 
    NativeBridge.copyToScala(batch.valuesPtr, amounts, 6)

    // IDs 50, 51, 52 * 10
    // Even indices hold the lower 32-bits (the actual values). Odd indices hold the upper 32-bits (0).
    amounts(0) shouldBe 500
    amounts(2) shouldBe 510
    amounts(4) shouldBe 520

    materializeOp.close()
  }
}