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
import scala.collection.Seq

class MorselExecSpec extends AnyFlatSpec with Matchers {
  
  "MorselExec" should "correctly aggregate results in parallel" in {
    val blocks = (1L to 1000L).toSeq
    val scanFunc = (ptr: Long) => ptr.toInt
    val result = MorselExec.scanParallel(blocks, scanFunc)
    result shouldBe 500500
  }

  it should "handle empty input gracefully" in {
    MorselExec.scanParallel(Seq.empty, _ => 1) shouldBe 0
  }

  it should "demonstrate speedup over sequential execution" in {
    val numBlocks = 4000
    val blocks = (0L until numBlocks.toLong).toSeq
    
    val heavyTask = (ptr: Long) => {
      var acc = 0.0
      var i = 0
      while (i < 5000) { 
        acc += Math.sin(i + ptr) * Math.cos(i)
        i += 1
      }
      1 
    }

    println(f"\n=== MORSEL BENCHMARK ($numBlocks Blocks) ===")
    val t0 = System.nanoTime()
    var seqSum = 0
    blocks.foreach { b => seqSum += heavyTask(b) }
    val t1 = System.nanoTime()
    
    val t2 = System.nanoTime()
    val parSum = MorselExec.scanParallel(blocks, heavyTask)
    val t3 = System.nanoTime()
    
    val seqTime = (t1 - t0) / 1e6
    val parTime = (t3 - t2) / 1e6
    val speedup = seqTime / parTime
    
    println(f"Sequential: $seqTime%.2f ms")
    println(f"MorselExec: $parTime%.2f ms")
    println(f"Speedup:    $speedup%.2fx")
    
    if (MorselExec.activeCores > 2) speedup should be > 1.5
  }
}