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

package org.awandb.server

import java.util.concurrent.ThreadLocalRandom

// Config hook
object AwanConfig {
  var enableProfiler: Boolean = false
}

// Global Profiling System 
object AwanDBInternalProfiler {

  // ThreadLocal so concurrent Flight streams don't overwrite each other
  // arr(0) = Global Start, arr(1) = Phase 1 Dur, arr(2) = Phase 2 Dur, arr(3) = Phase 3 Dur, arr(4) = Last Marker
  private val timerLocal = new ThreadLocal[Array[Long]]() {
    override def initialValue(): Array[Long] = new Array[Long](5)
  }

  // To group and identify requests
  private val reqIdLocal = new ThreadLocal[String]()

  @inline def start(reqId: String = f"Q-${ThreadLocalRandom.current().nextInt(0xFFFFF)}%05x"): Unit = {
    if (!AwanConfig.enableProfiler) return
    reqIdLocal.set(reqId)
    val arr = timerLocal.get()
    val now = System.currentTimeMillis()
    arr(0) = now // Global Start
    arr(1) = 0L  // Reset durations
    arr(2) = 0L
    arr(3) = 0L
    arr(4) = now // Set first marker
  }

  @inline def stampPhase1(): Unit = {
    if (!AwanConfig.enableProfiler) return
    val now = System.currentTimeMillis()
    val arr = timerLocal.get()
    arr(1) = now - arr(4) // Record Phase 1 duration
    arr(4) = now          // Update marker
  }

  @inline def stampPhase2(): Unit = {
    if (!AwanConfig.enableProfiler) return
    val now = System.currentTimeMillis()
    val arr = timerLocal.get()
    arr(2) = now - arr(4) // Record Phase 2 duration
    arr(4) = now          // Update marker
  }

  @inline def finish(): Unit = {
    if (!AwanConfig.enableProfiler) return
    val now = System.currentTimeMillis()
    val arr = timerLocal.get()
    val threadId = Thread.currentThread().getId
    
    val totalTime = if (arr(0) > 0) now - arr(0) else 0
    arr(3) = now - arr(4) // Record Phase 3 duration (or failure duration)
    
    val reqId = reqIdLocal.get()

    println(s"🔍 [$reqId] Thread-$threadId | Total: ${totalTime}ms")
    println(s"  ├─ 🧠 AST Parse: ${arr(1)}ms")
    println(s"  ├─ ⚡ JNI Execution: ${arr(2)}ms")
    println(s"  └─ 🚀 Arrow Packing: ${arr(3)}ms")
    
    // Clean up to prevent ThreadLocal memory leaks
    timerLocal.remove()
    reqIdLocal.remove()
  }
}