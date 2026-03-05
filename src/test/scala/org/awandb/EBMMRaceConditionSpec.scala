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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class EBMMRaceConditionSpec extends AnyFunSuite with BeforeAndAfterAll {

  val tableName = "ebmm_race_test"
  val testDir = s"target/data_$tableName"

  override def beforeAll(): Unit = {
    NativeBridge.init()
    val dir = new java.io.File(testDir)
    if (dir.exists()) {
      deleteRecursively(dir)
    }
    dir.mkdirs()
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("EBMM prevents Use-After-Free during concurrent Compaction and Vector Search") {
    // We set daemonIntervalMs exceptionally high so we can manually trigger the attack via Thread 2
    val table = new AwanTable(tableName, 10_000, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)
    table.addColumn("embedding", isString = false, isVector = true)

    println("[Test] Seeding data...")
    for (i <- 0 until 1000) {
      table.insertRow(Array(i, Array.fill(128)(math.random().toFloat)))
    }
    table.flush()

    @volatile var running = true

    // THREAD 1: The Victim (Constant Vector Search)
    val readerFuture = Future {
      var iterations = 0
      while (running) {
        val targetVec = Array.fill(128)(0.5f)
        table.queryVector("embedding", targetVec, 0.5f)
        iterations += 1
      }
      iterations
    }

    // THREAD 2: The Attacker (Forced Compactions and Reclamation)
    val compactorFuture = Future {
      var compactions = 0
      while (running) {
        table.insertRow(Array(1000 + compactions, Array.fill(128)(0.1f)))
        table.delete(compactions % 1000) 
        table.flush()
        
        table.epochManager.advanceGlobalEpoch()
        table.compactor.compact(0.0) // Force merge all blocks
        table.epochManager.tryReclaim()
        
        compactions += 1
        Thread.sleep(2) // Aggressive race window
      }
      compactions
    }

    println("[Test] Threads fighting for 3 seconds...")
    Thread.sleep(3000)
    running = false

    val reads = Await.result(readerFuture, 10.seconds)
    val compactions = Await.result(compactorFuture, 10.seconds)

    println(s"[Test] Finished logic cleanly! Reads: $reads | Compactions: $compactions")
    
    assert(reads > 0, "No reads were performed")
    assert(compactions > 0, "No compactions were performed")

    // COOL DOWN: Let any pending JNI calls finish before closing native memory
    Thread.sleep(500) 
    table.close()
    println("[Test] Table closed safely.")
  }
}