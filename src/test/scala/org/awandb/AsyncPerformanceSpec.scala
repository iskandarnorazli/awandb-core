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

class AsyncPerformanceSpec extends AnyFlatSpec with Matchers {

  def clearData(dir: String): Unit = {
    val d = new File(dir)
    if (d.exists()) {
      d.listFiles().foreach(_.delete())
      d.delete()
    }
  }

  "AwanDB" should "flush fast and index in background" in {
    val rows = 5000000 // 5M rows
    val dir = "data/bench_async_5m"
    clearData(dir)
    
    println(f"\n=== ASYNC INDEX BENCHMARK: $rows%,d Rows ===")
    
    val data = Array.fill(rows)(Random.nextInt())
    // Enable Indexing (but it will be lazy)
    val table = new AwanTable("bench", rows, dir, enableIndex = true) 
    table.addColumn("col1")
    
    // 1. Batch Insert
    table.insertBatch(data)
    
    // 2. Measure Flush Time
    val t1 = System.nanoTime()
    table.flush()
    val flushTime = (System.nanoTime() - t1) / 1e6
    println(f"| Flush (Async)  | $flushTime%8.2f ms  <-- Expect < 2000ms")
    
    // 3. Race Condition / Availability Test
    // While the background worker is busy, can we still query?
    // It should fallback to scan (slow but correct)
    val t2 = System.nanoTime()
    table.query(Random.nextInt())
    val slowRead = (System.nanoTime() - t2) / 1e6
    println(f"| Read (Pending) | $slowRead%8.2f ms  <-- Fallback to Scan")

    // 4. Force Index Build (Simulate background worker finishing)
    println("| ... Background Worker Building Index ...")
    while(table.blockManager.buildPendingIndexes() > 0) {
        // busy wait
    }
    
    // 5. Indexed Read
    val t3 = System.nanoTime()
    table.query(Random.nextInt())
    val fastRead = (System.nanoTime() - t3) / 1e6
    println(f"| Read (Indexed) | $fastRead%8.2f ms  <-- O(1) Lookup")
    
    table.close()
  }
}