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

package org.awandb.core.pipeline

import org.awandb.core.engine.{AwanTable}
import org.awandb.core.pipeline.ContinuousPipeWorker
import org.awandb.core.jni.NativeBridge
import scala.collection.concurrent.TrieMap

object PipeManager {
  // Track running pipes so we can gracefully shut them down later
  private val runningPipes = new TrieMap[String, Thread]()

  def startPipe(pipeName: String, sourceTable: AwanTable, destTable: AwanTable): Unit = {
    if (runningPipes.contains(pipeName)) {
      throw new RuntimeException(s"Pipe '$pipeName' is already running.")
    }

    val pipeThread = new Thread(new Runnable {
      override def run(): Unit = {
        println(s"[PipeManager] 🚀 Starting continuous ETL pipe: $pipeName")
        
        // Grab the active block from the source table.
        // Assuming blockManager exposes the loaded blocks array (as seen in TableScanOperator)
        val blocks = sourceTable.blockManager.getLoadedBlocks
        if (blocks.isEmpty) return
        val sourceBlockPtr = blocks.last // Tail the most recent block

        val batchSize = 1000
        val colIdx = 0 // Tailing the primary key/first column for this MVP
        
        // Allocate native transfer buffer
        val transferBufferPtr = NativeBridge.allocMainStore(batchSize)
        
        val worker = new ContinuousPipeWorker(
          sourceBlockPtr = sourceBlockPtr,
          colIdx = colIdx,
          batchSize = batchSize,
          transferBufferPtr = transferBufferPtr
        )

        try {
          while (!Thread.currentThread().isInterrupted) {
            val rowsRead = worker.poll()
            
            if (rowsRead > 0) {
              // Read the native buffer into Scala
              val scalaBuf = new Array[Int](rowsRead)
              NativeBridge.copyToScala(transferBufferPtr, scalaBuf, rowsRead)
              
              // Write to destination table
              // (Mapping to Array[Any] to match your insertRow signature)
              scalaBuf.foreach { value =>
                destTable.insertRow(Array(value, 0, 0)) // Padding dummy cols if needed
              }
              
              // In debug mode, print out the pipe activity
              // println(s"[Pipe: $pipeName] Transferred $rowsRead rows. Offset: ${worker.getCurrentOffset}")
            } else {
              // Backoff if no new data to prevent burning CPU loops
              Thread.sleep(10)
            }
          }
        } catch {
          case _: InterruptedException => 
            println(s"[PipeManager] 🛑 Pipe '$pipeName' interrupted and shutting down.")
        } finally {
          NativeBridge.freeMainStore(transferBufferPtr)
          runningPipes.remove(pipeName)
        }
      }
    })

    pipeThread.setName(s"awan-pipe-$pipeName")
    pipeThread.setDaemon(true) // Ensure JVM can exit even if pipes are running
    pipeThread.start()
    
    runningPipes.put(pipeName, pipeThread)
  }

  def stopPipe(pipeName: String): Boolean = {
    runningPipes.get(pipeName) match {
      case Some(thread) =>
        thread.interrupt()
        true
      case None => false
    }
  }
}