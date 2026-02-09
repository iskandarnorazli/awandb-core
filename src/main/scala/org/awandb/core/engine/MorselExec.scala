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

package org.awandb.core.engine

import org.awandb.core.jni.NativeBridge
import java.util.concurrent.{ForkJoinPool, Callable, ExecutionException}
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object MorselExec {
  
  val hardwareInfo: (Int, Long) = try {
      NativeBridge.getHardwareInfo()
  } catch {
      case e: Throwable => (Runtime.getRuntime.availableProcessors(), 12 * 1024 * 1024L)
  }
  
  val cores: Int = hardwareInfo._1
  val activeCores = math.max(1, cores - 1)
  
  println(s"[MorselExec] Detected $cores Cores. Pool Size: $activeCores Threads.")

  val executor = new ForkJoinPool(activeCores)

  // --- GENERIC PARALLEL RUNNER [NEW] ---
  // Allows executing arbitrary DAGs (like HashAgg -> Scan) in parallel.
  // This is used by AwanTable.executeGroupBy to run one pipeline per core.
  def runParallel[T](tasks: Seq[Callable[T]]): Seq[T] = {
    if (tasks.isEmpty) return Seq.empty
    
    // Invoke all tasks in the pool
    val futures = executor.invokeAll(tasks.asJava)
    
    // Collect results and unwrap execution exceptions
    futures.asScala.toSeq.map { f =>
      try {
        f.get()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  class ScanTask(val blockPtr: Long, val scanFunc: Long => Int) extends Callable[Int] {
    override def call(): Int = {
      if (blockPtr == 0) 0 else scanFunc(blockPtr)
    }
  }

  // --- SINGLE VALUE AGGREGATION ---
  def scanParallel(blocks: Seq[Long], scanFunc: Long => Int): Int = {
    if (blocks.isEmpty) return 0
    val taskCount = blocks.size
    
    if (taskCount == 1) return scanFunc(blocks.head)

    val tasks = new ArrayList[Callable[Int]](taskCount)
    val it = blocks.iterator
    
    while (it.hasNext) {
       tasks.add(new ScanTask(it.next(), scanFunc))
    }

    val futures = executor.invokeAll(tasks)
    var total = 0
    val resIt = futures.iterator()
    
    while (resIt.hasNext) {
      try {
        total += resIt.next().get() 
      } catch {
        // [FIX] Unwrap ExecutionException to reveal the true crash reason
        case e: ExecutionException => throw e.getCause
      }
    }
    total
  }

  // --- ARRAY AGGREGATION ---
  def scanSharedParallel(
      blocks: Seq[Long], 
      allocator: () => Array[Int], 
      scanner: (Long, Array[Int]) => Unit
  ): Array[Int] = {
    
    if (blocks.isEmpty) return allocator()
    
    val tasks = new ArrayList[Callable[Array[Int]]](blocks.size)
    val it = blocks.iterator
    
    while (it.hasNext) {
       val ptr = it.next()
       tasks.add(new Callable[Array[Int]] {
         override def call(): Array[Int] = {
           val localCounts = allocator() 
           if (ptr != 0) {
             scanner(ptr, localCounts)
           }
           localCounts
         }
       })
    }
    
    val futures = executor.invokeAll(tasks)
    
    val finalCounts = allocator() 
    val len = finalCounts.length
    val resIt = futures.iterator()
    
    while (resIt.hasNext) {
       try {
         val partial = resIt.next().get()
         var i = 0
         while (i < len) {
           finalCounts(i) += partial(i)
           i += 1
         }
       } catch {
         // [FIX] Unwrap here too
         case e: ExecutionException => throw e.getCause
       }
    }
    finalCounts
  }
}