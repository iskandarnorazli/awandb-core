/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.awandb

import org.awandb.core.jni.NativeBridge
import org.awandb.core.engine.QueryExecutor
import org.awandb.core.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Random

class QueryExecutorSpec extends AnyFlatSpec with Matchers {

  val ROWS = 1_000_000
  val BATCH_SIZE = 4096

  "QueryExecutor" should "execute a full Scan -> Filter -> Agg -> Sort pipeline" in {
    
    // 1. DATA PREP (1 Million Rows, Low Cardinality)
    // Keys: 0 to 99,999 (approx 100k groups)
    val uniqueKeys = ROWS / 10
    val data = Array.fill(ROWS)(Random.nextInt(uniqueKeys)) 
    val dataPtr = NativeBridge.allocMainStore(ROWS)
    NativeBridge.loadData(dataPtr, data)
    
    // 2. SETUP FILTER (Match 50% of domain space)
    // Filter Range: 0 to 499,999
    // Since our data is 0..99,999, effectively 100% of data matches the filter in this specific case.
    // This stress tests the Aggregation and Sort phases with max data volume.
    val cuckooPtr = NativeBridge.cuckooCreate(ROWS / 2)
    val filterData = Array.range(0, ROWS / 2)
    val filterPtr = NativeBridge.allocMainStore(ROWS / 2)
    NativeBridge.loadData(filterPtr, filterData)
    NativeBridge.cuckooBuildBatch(cuckooPtr, filterData)

    // 3. BUILD DAG (The "Physical Plan")
    
    // A. Scan (Zero-Copy-ish)
    // Simulates reading blocks from disk/memory into execution batches
    val scanOp = new NativeScanOperator(dataPtr, ROWS, BATCH_SIZE)
    
    // B. Filter (SIP)
    // Drops rows not in the Cuckoo Filter (in this test, none are dropped)
    val filterOp = new SipFilterOperator(scanOp, cuckooPtr)
    
    // C. Aggregate (Hash Group By)
    // Condenses 1M rows -> ~100k rows (SUM/COUNT)
    val aggOp = new HashAggOperator(filterOp)
    
    // D. Sort (Radix)
    // Sorts the aggregated results by Key
    val sortOp = new SortOperator(aggOp, ROWS)

    // 4. EXECUTE
    val t0 = System.nanoTime()
    val resultCount = QueryExecutor.execute(sortOp)
    val t1 = System.nanoTime()
    
    println(s"Query Time: ${(t1 - t0) / 1e6} ms")
    println(s"Result Rows: $resultCount")

    // Cleanup
    NativeBridge.freeMainStore(dataPtr)
    NativeBridge.freeMainStore(filterPtr)
    NativeBridge.cuckooDestroy(cuckooPtr)
    
    // VALIDATION
    // We expect roughly 'uniqueKeys' groups. 
    // Since we generated 1M rows over 100k keys uniformly, nearly every key should be present.
    resultCount should be > (uniqueKeys * 0.99).toLong
    resultCount should be <= uniqueKeys.toLong
  }
}

// Helper for the test to properly move data
class NativeScanOperator(dataPtr: Long, totalRows: Int, batchSize: Int) extends Operator {
  private var pos = 0
  private var batch: VectorBatch = _

  override def open(): Unit = { batch = new VectorBatch(batchSize) }

  override def next(): VectorBatch = {
    if (pos >= totalRows) return null
    val len = math.min(batchSize, totalRows - pos)
    
    // 1. Calculate source address in the Main Store
    val offset = pos * 4L // Use Long to prevent overflow
    val viewPtr = NativeBridge.getOffsetPointer(dataPtr, offset)

    // 2. Perform Native Memcpy (Zero-Copy from Java's perspective)
    // We move data from the "Storage Block" (viewPtr) to the "Execution Batch" (keysPtr)
    NativeBridge.memcpy(viewPtr, batch.keysPtr, len * 4L)
    
    batch.count = len
    pos += len
    batch
  }
  override def close(): Unit = batch.close()
}