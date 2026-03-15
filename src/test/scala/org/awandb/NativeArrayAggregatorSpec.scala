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

package org.awandb.core.query

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.jni.NativeBridge
import org.awandb.core.engine.memory.NativeMemoryTracker

class NativeArrayAggregatorSpec extends AnyFunSuite {

  test("Phase 4: O(1) Branchless Array Aggregation for Single Dictionary Column") {
    val dictIds = Array(0, 1, 0, 2, 1, 0)
    val vals = Array(10, 20, 15, 5, 30, 25)
    val count = dictIds.length
    val maxDictId = 2

    val keysPtr = NativeBridge.allocMainStore(count * 4L)
    val valsPtr = NativeBridge.allocMainStore(count * 4L)
    NativeBridge.loadData(keysPtr, dictIds)
    NativeBridge.loadData(valsPtr, vals)

    val aggPtr = NativeBridge.aggregateArraySum(keysPtr, valsPtr, count, maxDictId)
    assert(aggPtr != 0L)

    val outKeysPtr = NativeBridge.allocMainStore(3 * 4L)
    val outValsPtr = NativeBridge.allocMainStore(3 * 8L) 
    val exportedCount = NativeBridge.aggregateArrayExport(aggPtr, outKeysPtr, outValsPtr)
    
    assert(exportedCount == 3)

    val resultKeys = new Array[Int](3)
    val resultVals = new Array[Long](3)
    NativeBridge.copyToScala(outKeysPtr, resultKeys, 3)
    NativeBridge.copyLongsToScala(outValsPtr, resultVals, 3)

    val resultMap = resultKeys.zip(resultVals).toMap
    assert(resultMap(0) == 50L)
    assert(resultMap(1) == 50L)
    assert(resultMap(2) == 5L)

    NativeBridge.freeArrayAggregationResult(aggPtr)
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valsPtr)
    NativeBridge.freeMainStore(outKeysPtr)
    NativeBridge.freeMainStore(outValsPtr)

    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: Composite Key Packing for Multi-Column Dictionary Group-By") {
    val catIds = Array(1, 10, 1)
    val storeIds = Array(2, 5, 2)
    val vals = Array(100, 200, 50)
    
    val packedKeys = catIds.zip(storeIds).map { case (c, s) => (c << 16) | s }
    val maxPackedId = (10 << 16) | 5
    val count = packedKeys.length

    val keysPtr = NativeBridge.allocMainStore(count * 4L)
    val valsPtr = NativeBridge.allocMainStore(count * 4L)
    NativeBridge.loadData(keysPtr, packedKeys)
    NativeBridge.loadData(valsPtr, vals)

    val aggPtr = NativeBridge.aggregateArraySum(keysPtr, valsPtr, count, maxPackedId)
    assert(aggPtr != 0L)

    val outKeysPtr = NativeBridge.allocMainStore(2 * 4L)
    val outValsPtr = NativeBridge.allocMainStore(2 * 8L)
    val exportedCount = NativeBridge.aggregateArrayExport(aggPtr, outKeysPtr, outValsPtr)
    
    assert(exportedCount == 2)

    val resultKeys = new Array[Int](2)
    val resultVals = new Array[Long](2)
    NativeBridge.copyToScala(outKeysPtr, resultKeys, 2)
    NativeBridge.copyLongsToScala(outValsPtr, resultVals, 2)

    val resultMap = resultKeys.zip(resultVals).toMap
    assert(resultMap((1 << 16) | 2) == 150L)
    assert(resultMap((10 << 16) | 5) == 200L)

    NativeBridge.freeArrayAggregationResult(aggPtr)
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valsPtr)
    NativeBridge.freeMainStore(outKeysPtr)
    NativeBridge.freeMainStore(outValsPtr)
    
    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: 64-bit Overflow Safety (Values Exceeding Max Int)") {
    val dictIds = Array(5, 5, 5)
    // 2 Billion x 3 = 6 Billion (Requires int64_t to prevent overflow)
    val vals = Array(2000000000, 2000000000, 2000000000) 
    
    val count = dictIds.length
    val maxDictId = 5

    val keysPtr = NativeBridge.allocMainStore(count * 4L)
    val valsPtr = NativeBridge.allocMainStore(count * 4L)
    NativeBridge.loadData(keysPtr, dictIds)
    NativeBridge.loadData(valsPtr, vals)

    val aggPtr = NativeBridge.aggregateArraySum(keysPtr, valsPtr, count, maxDictId)
    val outKeysPtr = NativeBridge.allocMainStore(1 * 4L)
    val outValsPtr = NativeBridge.allocMainStore(1 * 8L) 
    
    val exportedCount = NativeBridge.aggregateArrayExport(aggPtr, outKeysPtr, outValsPtr)
    assert(exportedCount == 1)

    val resultVals = new Array[Long](1)
    NativeBridge.copyLongsToScala(outValsPtr, resultVals, 1)

    assert(resultVals(0) == 6000000000L, "Failed to accumulate into 64-bit integer safely")

    NativeBridge.freeArrayAggregationResult(aggPtr)
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valsPtr)
    NativeBridge.freeMainStore(outKeysPtr)
    NativeBridge.freeMainStore(outValsPtr)

    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: Sparse Aggregation (Large Dictionary, Few Rows)") {
    // A dictionary with 1 million capacity, but only 2 items are grouped
    val maxDictId = 1000000 
    val dictIds = Array(0, 999999)
    val vals = Array(42, 84)
    val count = dictIds.length

    val keysPtr = NativeBridge.allocMainStore(count * 4L)
    val valsPtr = NativeBridge.allocMainStore(count * 4L)
    NativeBridge.loadData(keysPtr, dictIds)
    NativeBridge.loadData(valsPtr, vals)

    val aggPtr = NativeBridge.aggregateArraySum(keysPtr, valsPtr, count, maxDictId)
    
    val outKeysPtr = NativeBridge.allocMainStore(2 * 4L)
    val outValsPtr = NativeBridge.allocMainStore(2 * 8L) 
    
    // The export function should skip the 999,998 empty slots instantly
    val exportedCount = NativeBridge.aggregateArrayExport(aggPtr, outKeysPtr, outValsPtr)
    assert(exportedCount == 2)

    val resultKeys = new Array[Int](2)
    NativeBridge.copyToScala(outKeysPtr, resultKeys, 2)
    
    assert(resultKeys.contains(0))
    assert(resultKeys.contains(999999))

    NativeBridge.freeArrayAggregationResult(aggPtr)
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valsPtr)
    NativeBridge.freeMainStore(outKeysPtr)
    NativeBridge.freeMainStore(outValsPtr)

    NativeMemoryTracker.assertNoLeaks()
  }

  test("Phase 4: Empty Data Batch Handling") {
    // If a query filters out all rows, Volcano batch count will be 0
    val count = 0
    val maxDictId = 10

    val keysPtr = NativeBridge.allocMainStore(16L) // Dummy allocations
    val valsPtr = NativeBridge.allocMainStore(16L)

    // The JNI binding should intercept count <= 0 and return 0
    val aggPtr = NativeBridge.aggregateArraySum(keysPtr, valsPtr, count, maxDictId)
    assert(aggPtr == 0L, "Engine should return 0 pointer for empty batches")

    // Exporting from a 0 pointer should yield 0 rows safely
    val exportedCount = NativeBridge.aggregateArrayExport(aggPtr, keysPtr, valsPtr)
    assert(exportedCount == 0)

    NativeBridge.freeArrayAggregationResult(aggPtr) // Should be a safe no-op
    NativeBridge.freeMainStore(keysPtr)
    NativeBridge.freeMainStore(valsPtr)

    NativeMemoryTracker.assertNoLeaks()
  }
}