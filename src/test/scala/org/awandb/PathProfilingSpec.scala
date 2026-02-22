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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import java.io.File
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class PathProfilingSpec extends AnyFlatSpec with Matchers {

  def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        val children = file.listFiles()
        if (children != null) children.foreach(deleteRecursively)
      }
      file.delete()
    }
  }

  val SIZES = Seq(1_000, 100_000, 1_000_000, 10_000_000)
  val TEST_DIR = "data/profiling_bench"

  "AwanDB Profiler" should "measure Hot vs Cold paths in Single-Threaded mode" in {
    println("\n=====================================================================================================")
    println("                                  SINGLE-THREADED PATH PROFILING                                     ")
    println("=====================================================================================================")
    println(f"| ${"ROWS"}%-10s | ${"PHASE / PATH"}%-25s | ${"TIME (ms)"}%10s | ${"THROUGHPUT"}%15s | ${"BANDWIDTH"}%12s |")
    println("-----------------------------------------------------------------------------------------------------")

    for (rows <- SIZES) {
      val dir = s"$TEST_DIR/single_$rows"
      deleteRecursively(new File(dir))

      val table = new AwanTable(s"profile_s_$rows", rows, dir)
      table.addColumn("val")
      
      // 1. SCALAR INGESTION (API Overhead)
      val t0 = System.nanoTime()
      var i = 0
      while (i < rows) {
        table.insert(i)
        i += 1
      }
      val t1 = System.nanoTime()
      printMetric(rows, "1. Scalar Insert API", t1 - t0)

      // 2. HOT PATH QUERY (Pure JVM ArrayBuffer Scan)
      val searchVal = rows / 2
      val t2 = System.nanoTime()
      val hotMatches = table.query(searchVal) // Using Range threshold
      val t3 = System.nanoTime()
      printMetric(rows, "2. Hot Path Query (RAM)", t3 - t2)
      
      // 3. FLUSH OVERHEAD (JNI + Morsel Chunking)
      val t4 = System.nanoTime()
      table.flush()
      val t5 = System.nanoTime()
      printMetric(rows, "3. Flush (RAM -> C++)", t5 - t4)

      // 4. COLD PATH QUERY (Pure C++ AVX Kernel via MorselExec)
      val t6 = System.nanoTime()
      val coldMatches = table.query(searchVal) 
      val t7 = System.nanoTime()
      printMetric(rows, "4. Cold Path Query (Disk)", t7 - t6)

      println("-----------------------------------------------------------------------------------------------------")
      table.close()
    }
  }

  it should "measure Vectorized and Multi-Threaded limits" in {
    println("\n=====================================================================================================")
    println("                                 MULTI-THREADED & VECTORIZED PROFILING                               ")
    println("=====================================================================================================")
    println(f"| ${"ROWS"}%-10s | ${"PHASE / PATH"}%-25s | ${"TIME (ms)"}%10s | ${"THROUGHPUT"}%15s | ${"BANDWIDTH"}%12s |")
    println("-----------------------------------------------------------------------------------------------------")

    for (rows <- SIZES) {
      val dir = s"$TEST_DIR/multi_$rows"
      deleteRecursively(new File(dir))

      val table = new AwanTable(s"profile_m_$rows", rows, dir)
      table.addColumn("val")

      val dataArray = (0 until rows).toArray

      // 1. VECTORIZED INGESTION (Zero API Overhead, Direct Memcopy)
      val t0 = System.nanoTime()
      table.insertBatch(dataArray)
      val t1 = System.nanoTime()
      printMetric(rows, "1. Vectorized Batch Insert", t1 - t0)

      table.flush()

      // 2. MULTI-THREADED COLD READS (Simulating 16 concurrent users)
      val concurrentQueries = 16
      val queryValue = rows / 2

      val t2 = System.nanoTime()
      val futures = (0 until concurrentQueries).map { _ =>
        Future { table.query(queryValue) }
      }
      Await.result(Future.sequence(futures), 30.seconds)
      val t3 = System.nanoTime()
      
      // Divide by concurrentQueries to get the average time per query under load
      val avgTimeNs = (t3 - t2) / concurrentQueries
      printMetric(rows, s"2. Concurrent Reads ($concurrentQueries)", avgTimeNs)

      println("-----------------------------------------------------------------------------------------------------")
      table.close()
    }
  }

  // --- HELPER: Formats and calculates Hardware Metrics ---
  private def printMetric(rows: Int, phase: String, nanos: Long): Unit = {
    val ms = nanos / 1_000_000.0
    val secs = nanos / 1_000_000_000.0
    val mops = if (secs > 0) (rows / 1_000_000.0) / secs else 0.0
    val bytesProcessed = rows * 4L // 4 bytes per Int
    val gbps = if (secs > 0) (bytesProcessed / 1073741824.0) / secs else 0.0

    // Prevent infinity in prints for extremely fast operations (< 0.01 ms)
    val printMops = if (nanos < 10000) "MAX" else f"$mops%10.2f M/s"
    val printGbps = if (nanos < 10000) "MAX" else f"$gbps%9.2f GB/s"

    val rowStr = if (rows >= 1000000) s"${rows/1000000}M" else s"${rows/1000}K"

    println(f"| $rowStr%-10s | $phase%-25s | $ms%10.3f | $printMops%15s | $printGbps%12s |")
  }
}