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

import org.awandb.core.engine.MorselExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Seq

class MorselExecSpec extends AnyFlatSpec with Matchers {
  
  // --- 1. BASIC CORRECTNESS ---
  "MorselExec" should "correctly aggregate results in parallel" in {
    val blocks = (1L to 1000L).toSeq
    val result = MorselExec.scanParallel(blocks, ptr => ptr.toInt)
    result shouldBe 500500
  }

  it should "handle empty input gracefully" in {
    MorselExec.scanParallel(Seq.empty, _ => 1) shouldBe 0
    val res = MorselExec.scanSharedParallel(
        Seq.empty, 
        () => new Array[Int](1), 
        (p, c) => ()
    )
    res.length shouldBe 1
    res(0) shouldBe 0
  }

  // --- 2. REGRESSION TEST FOR "16x BUG" ---
  it should "execute each task exactly ONCE (No Broadcasting)" in {
    val numBlocks = 1000
    val blocks = (1L to numBlocks.toLong).toSeq
    val executionCounter = new AtomicInteger(0)

    MorselExec.scanParallel(blocks, { _ => 
      executionCounter.incrementAndGet()
      1
    })

    executionCounter.get() shouldBe numBlocks
  }

  // --- 3. SHARED SCAN (VECTORIZED REDUCTION) ---
  it should "correctly reduce arrays in scanSharedParallel" in {
    // [FIXED] Use '1L to 100L' (inclusive). 
    // '0L' is treated as NULL by the engine and skipped, causing the off-by-one error.
    val blocks = (1L to 100L).toSeq 
    val arraySize = 3
    
    val results = MorselExec.scanSharedParallel(
      blocks,
      allocator = () => new Array[Int](arraySize),
      scanner = (ptr, counts) => {
         // Mock logic: Add 1 to every bucket
         counts(0) = 1
         counts(1) = 1
         counts(2) = 1
      }
    )
    
    // 100 blocks * 1 per block = 100
    results(0) shouldBe 100
    results(1) shouldBe 100
    results(2) shouldBe 100
  }

  // --- 4. SAFETY & ERROR HANDLING ---
  it should "propagate exceptions from worker threads" in {
    val blocks = (1L to 100L).toSeq
    // We expect the unwrapped RuntimeException, not ExecutionException
    assertThrows[RuntimeException] {
      MorselExec.scanParallel(blocks, { ptr =>
        if (ptr == 50) throw new RuntimeException("Worker Crash Test")
        1
      })
    }
  }

  // --- 5. PERFORMANCE BENCHMARK ---
  it should "demonstrate speedup over sequential execution" in {
    if (MorselExec.activeCores >= 2) {
      val numBlocks = 2000
      val blocks = (0L until numBlocks.toLong).toSeq
      
      val heavyTask = (ptr: Long) => {
        var acc = 0.0
        var i = 0
        while (i < 10000) { 
          acc += Math.sin(i + ptr) * Math.cos(i)
          i += 1
        }
        1 
      }

      val t0 = System.nanoTime()
      blocks.foreach(heavyTask)
      val t1 = System.nanoTime()
      
      val t2 = System.nanoTime()
      MorselExec.scanParallel(blocks, heavyTask)
      val t3 = System.nanoTime()
      
      val seqTime = (t1 - t0) / 1e6
      val parTime = (t3 - t2) / 1e6
      val speedup = seqTime / parTime
      
      println(f"Speedup: $speedup%.2fx (Seq: $seqTime%.2f ms, Par: $parTime%.2f ms)")
      speedup should be > 1.1
    }
  }
}