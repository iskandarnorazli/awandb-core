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

import org.awandb.core.jni.NativeBridge
import java.util.concurrent.{ForkJoinPool, Callable}
import java.util.ArrayList
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object MorselExec {
  
  // 1. HARDWARE DISCOVERY
  // [FIX] Explicit type annotation solves the "recursive value" error
  val hardwareInfo: (Int, Long) = try {
     NativeBridge.getHardwareInfo()
  } catch {
     case e: Throwable => (Runtime.getRuntime.availableProcessors(), 12 * 1024 * 1024L)
  }
  
  val cores: Int = hardwareInfo._1
  val l3CacheBytes: Long = hardwareInfo._2
  
  val activeCores = math.max(1, cores - 1)
  
  println(s"[MorselExec] Detected $cores Cores, L3 Cache: ${l3CacheBytes / 1024 / 1024} MB")
  println(s"[MorselExec] Parallelism Level: $activeCores Threads")

  val executor = new ForkJoinPool(activeCores)

  // ... (Rest of scanParallel / scanSharedParallel logic) ...
  // (Paste the logic from your previous snippet here)
  
  def scanParallel(blocks: Seq[Long], scanFunc: Long => Int): Int = {
    if (blocks.isEmpty) return 0
    val taskCount = blocks.size
    if (taskCount == 1) return scanFunc(blocks.head)

    val tasks = new ArrayList[Callable[Int]](taskCount)
    val it = blocks.iterator
    while (it.hasNext) {
       val ptr = it.next()
       tasks.add(new Callable[Int] {
         override def call(): Int = scanFunc(ptr)
       })
    }

    val futures = executor.invokeAll(tasks)
    var total = 0
    val resIt = futures.iterator()
    while (resIt.hasNext) total += resIt.next().get() 
    total
  }

  def scanSharedParallel(blocks: Seq[Long], thresholds: Array[Int]): Array[Int] = {
    if (blocks.isEmpty) return new Array[Int](thresholds.length)
    val tasks = new ArrayList[Callable[Array[Int]]](blocks.size)
    val len = thresholds.length
    val it = blocks.iterator
    while (it.hasNext) {
       val ptr = it.next()
       tasks.add(new Callable[Array[Int]] {
         override def call(): Array[Int] = {
           val localCounts = new Array[Int](len)
           if (ptr > 100000) NativeBridge.avxScanMultiBlock(ptr, 0, thresholds, localCounts)
           localCounts
         }
       })
    }
    val futures = executor.invokeAll(tasks)
    val finalCounts = new Array[Int](len)
    val resIt = futures.iterator()
    while (resIt.hasNext) {
       val partial = resIt.next().get()
       var i = 0
       while (i < len) {
         finalCounts(i) += partial(i)
         i += 1
       }
    }
    finalCounts
  }
}