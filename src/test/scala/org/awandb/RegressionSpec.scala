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

package org.awandb.core.regression

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MemoryAndConcurrencyRegressionSpec extends AnyFlatSpec with Matchers {

  "AwanDB Native Core" should "handle large-scale sorting without stack corruption" in {
    // Stress test the parallel radix sort which has a 64-thread hardcoded limit
    val largeSize = 10_000_000
    val ptr = NativeBridge.allocMainStore(largeSize)
    try {
      // FIX: Use radixSort instead of radixSortNative
      NativeBridge.radixSort(ptr, largeSize)
    } finally {
      NativeBridge.freeMainStore(ptr)
    }
  }

  it should "not crash during memory-intensive Aggregation/Join allocations" in {
    // Force a large allocation in the NativeHashMap to test null-checks
    // On Linux CI, if this is not checked in C++, it triggers SEGV at 0x0c
    val rows = 5_000_000
    val keysPtr = NativeBridge.allocMainStore(rows)
    val valsPtr = NativeBridge.allocMainStore(rows)
    
    try {
      // FIX: Use aggregateSum and freeAggregationResult
      val mapPtr = NativeBridge.aggregateSum(keysPtr, valsPtr, rows)
      if (mapPtr != 0) NativeBridge.freeAggregationResult(mapPtr)
    } finally {
      NativeBridge.freeMainStore(keysPtr)
      NativeBridge.freeMainStore(valsPtr)
    }
  }

  it should "survive high-concurrency dirty-block scans" in {
    val table = new AwanTable("concurrency_stress", 1_000_000, "data/stress")
    table.addColumn("val")
    
    // Fill with data and flush
    val data = (0 until 1_000_000).toArray
    table.insertBatch(data)
    table.flush()

    // Create "Dirty" state (high bitmap/bitmask activity)
    (0 until 100_000 by 2).foreach(id => table.delete(id))

    // Concurrent query stress: 32 threads performing AVX scans simultaneously
    val threads = 32
    val queries = (0 until threads).map { _ =>
      Future {
        table.query(500_000)
      }
    }

    // This captures race conditions in NativeBitmask sync or null blockPtr access
    Await.result(Future.sequence(queries), 60.seconds)
    table.close()
  }

  it should "survive massive hash map resizing (High Cardinality Aggregation)" in {
    // WHY WE TEST THIS: NativeHashMap starts at 8192 capacity. 
    // Passing millions of UNIQUE keys forces the C++ resize() function to trigger repeatedly.
    // This catches double-frees, dangling pointers, and std::bad_alloc during mid-operation rehashes.
    val rows = 2_000_000
    val keysPtr = NativeBridge.allocMainStore(rows)
    val valsPtr = NativeBridge.allocMainStore(rows)
    
    try {
      // 100% unique keys to force max collisions and resizes
      val uniqueKeys = (0 until rows).toArray
      val ones = Array.fill(rows)(1)
      
      NativeBridge.loadData(keysPtr, uniqueKeys)
      NativeBridge.loadData(valsPtr, ones)

      val mapPtr = NativeBridge.aggregateSum(keysPtr, valsPtr, rows)
      if (mapPtr != 0) NativeBridge.freeAggregationResult(mapPtr)
    } finally {
      NativeBridge.freeMainStore(keysPtr)
      NativeBridge.freeMainStore(valsPtr)
    }
  }

  it should "handle concurrent inserts, flushes, and queries without RAM-to-Disk corruption" in {
    // WHY WE TEST THIS: When table.flush() moves data from RAM (deltaIntBuffer) to Disk (Blocks),
    // it resets the RAM buffers. If the ReentrantReadWriteLock has a flaw, concurrent queries 
    // will read from cleared buffers or access destroyed Native Bitmaps, causing JVM crashes.
    val table = new AwanTable("rw_stress", 100_000, "data/rw_stress")
    table.addColumn("val")

    val isRunning = new java.util.concurrent.atomic.AtomicBoolean(true)

    // 1 Writer Thread: Constantly inserting and aggressively flushing to trigger RAM-to-Disk handoffs
    val writer = Future {
      var i = 0
      while (isRunning.get() && i < 500_000) {
        table.insertBatch(Array.fill(1000)(i))
        if (i % 5000 == 0) table.flush()
        i += 1000
      }
    }

    // 8 Reader Threads: Hammering the query engine while data is actively moving
    val readers = (0 until 8).map { _ =>
      Future {
        while (isRunning.get()) {
          table.query(500) // Constantly scan both RAM and Disk blocks
        }
      }
    }

    // Wait for writer to finish its stress loop, then shut down readers
    Await.ready(writer, 60.seconds)
    isRunning.set(false)
    Await.ready(Future.sequence(readers), 5.seconds)
    table.close()
  }

  it should "safely handle native dictionary encoding memory pressure" in {
    // WHY WE TEST THIS: The Native Dictionary allocates string pools in C++.
    // Pushing 1 million unique strings ensures the C++ string allocator handles 
    // massive heap expansions without throwing silent OOMs or overwriting bounds.
    val dictPtr = NativeBridge.dictionaryCreate()
    try {
      val batchSize = 1_000_000
      val batch = (0 until batchSize).map(i => s"unique_str_val_$i").toArray
      val outIdsPtr = NativeBridge.allocMainStore(batchSize)
      
      try {
        NativeBridge.dictionaryEncodeBatch(dictPtr, batch, outIdsPtr)
      } finally {
        NativeBridge.freeMainStore(outIdsPtr)
      }
    } finally {
      NativeBridge.dictionaryDestroy(dictPtr)
    }
  }

  it should "not cause General Protection Faults during Vector/Cosine AVX alignment" in {
    // WHY WE TEST THIS: AVX/SIMD instructions (_mm256_load_ps) require 32-byte aligned memory.
    // If block creation padding is slightly off, loading vector chunks throws a SEGV.
    val rows = 10_000
    val dim = 1536 // Standard OpenAI embedding size
    
    // Calculate bytes safely
    val colSizes = Array(rows * dim * 4) 
    val blockPtr = NativeBridge.createBlock(rows, 1, colSizes)
    
    try {
      val dummyData = Array.fill(rows * dim)(0.5f)
      NativeBridge.loadVectorData(blockPtr, 0, dummyData, dim)

      val queryVec = Array.fill(dim)(0.1f)
      val outIndicesPtr = NativeBridge.allocMainStore(rows)
      
      try {
        NativeBridge.avxScanVectorCosine(blockPtr, 0, queryVec, 0.8f, outIndicesPtr)
      } finally {
        NativeBridge.freeMainStore(outIndicesPtr)
      }
    } finally {
      if (blockPtr != 0) NativeBridge.freeMainStore(blockPtr)
    }
  }

  it should "gracefully handle Empty Join Probes and Missing Keys" in {
    // WHY WE TEST THIS: The Join Hash Map expects to find keys. If we probe 
    // a massive array of keys that completely miss, we test the infinite loop 
    // conditions of the linear probing logic in join.cpp.
    val buildRows = 10_000
    val probeRows = 500_000

    // Keys and Indices are 32-bit (Int)
    val buildKeysPtr = NativeBridge.allocMainStore(buildRows)
    val probeKeysPtr = NativeBridge.allocMainStore(probeRows)
    val outIndicesPtr = NativeBridge.allocMainStore(probeRows)

    // [CRITICAL FIX] Payloads are 64-bit (Long), so we MUST multiply by 2!
    val buildValsPtr = NativeBridge.allocMainStore(buildRows * 2)
    val outPayloadsPtr = NativeBridge.allocMainStore(probeRows * 2)

    try {
      // Build keys 0 to 9,999
      NativeBridge.loadData(buildKeysPtr, (0 until buildRows).toArray)
      
      // Probe keys 10,000 to 510,000 (100% MISS RATE)
      NativeBridge.loadData(probeKeysPtr, (buildRows until buildRows + probeRows).toArray)

      val mapPtr = NativeBridge.joinBuild(buildKeysPtr, buildValsPtr, buildRows)
      if (mapPtr != 0) {
        try {
          val matches = NativeBridge.joinProbe(mapPtr, probeKeysPtr, probeRows, outPayloadsPtr, outIndicesPtr)
          matches shouldBe 0
        } finally {
          NativeBridge.joinDestroy(mapPtr)
        }
      }
    } finally {
      NativeBridge.freeMainStore(buildKeysPtr)
      NativeBridge.freeMainStore(buildValsPtr)
      NativeBridge.freeMainStore(probeKeysPtr)
      NativeBridge.freeMainStore(outPayloadsPtr)
      NativeBridge.freeMainStore(outIndicesPtr)
    }
  }
}