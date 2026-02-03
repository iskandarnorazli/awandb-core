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

package org.awandb.core.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random

class WriteFusionPerformanceSpec extends AnyFlatSpec with Matchers {

  // Using a separate directory to avoid messing with other tests
  val TEST_DIR = "data/bench_write_fusion"
  
  // 5 Million Rows (approx 20MB of raw data)
  // Large enough to make the IO/Lock overhead visible
  val ROW_COUNT = 5000000 

  def clearData(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      // Delete WAL files and directory
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  "AwanDB" should "benchmark Write Fusion (Batch) vs Standard Write" in {
    println(s"\n--- WRITE FUSION BENCHMARK ($ROW_COUNT rows) ---")
    
    // 1. Generate Random Data
    // We do this outside the timer to measure ONLY the write speed
    val data = Array.fill(ROW_COUNT)(Random.nextInt())
    println("Data Generation Complete.")

    // -----------------------------------------------------------
    // TEST 1: STANDARD INSERT (Loop)
    // -----------------------------------------------------------
    clearData()
    val table1 = new AwanTable("bench_std", ROW_COUNT, TEST_DIR)
    table1.addColumn("col1")
    
    // Warmup JIT slightly (optional, but good practice)
    table1.insert(1) 
    
    println("Starting Standard Insert Loop...")
    val start1 = System.nanoTime()
    
    var i = 0
    while(i < ROW_COUNT) {
        table1.insert(data(i))
        i += 1
    }
    
    // Force physical sync to make comparison fair (include IO latency)
    table1.wal.sync() 
    
    val dur1 = (System.nanoTime() - start1) / 1e9
    val tps1 = ROW_COUNT / dur1
    val bw1 = (ROW_COUNT * 4.0) / (1024 * 1024) / dur1 // MB/s
    
    table1.close()
    println(f"Standard Loop Done: $dur1%.4f s")

    // -----------------------------------------------------------
    // TEST 2: FUSED WRITE (Batch)
    // -----------------------------------------------------------
    clearData()
    val table2 = new AwanTable("bench_fused", ROW_COUNT, TEST_DIR)
    table2.addColumn("col1")
    table2.insert(1) // Warmup
    
    println("Starting Fused Batch Insert...")
    val start2 = System.nanoTime()
    
    // THE MAGIC LINE: One Lock, One Syscall, One Memcpy
    table2.insertBatch(data)
    
    // Force physical sync
    table2.wal.sync()
    
    val dur2 = (System.nanoTime() - start2) / 1e9
    val tps2 = ROW_COUNT / dur2
    val bw2 = (ROW_COUNT * 4.0) / (1024 * 1024) / dur2 // MB/s
    
    table2.close()
    println(f"Fused Write Done:   $dur2%.4f s")

    // -----------------------------------------------------------
    // REPORT
    // -----------------------------------------------------------
    println(f"""
    |================================================================================
    | METHOD               | TIME (s)        | THROUGHPUT (M/s) | BANDWIDTH (MB/s) |
    |--------------------------------------------------------------------------------
    | Standard Insert      | $dur1%10.4f s    | ${tps1/1e6}%10.2f       | $bw1%10.2f       |
    | Fused Batch Insert   | $dur2%10.4f s    | ${tps2/1e6}%10.2f       | $bw2%10.2f       |
    |--------------------------------------------------------------------------------
    | SPEEDUP FACTOR: ${tps2/tps1}%.2fx faster
    |================================================================================
    """.stripMargin)
    
    // Cleanup
    clearData()
  }
}