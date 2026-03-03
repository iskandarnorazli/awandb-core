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

package org.awandb.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.{ByteArrayOutputStream, PrintStream}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class AwanDBInternalProfilerTest extends AnyFunSuite with Matchers {

  // 🛠️ Helper to capture Console.println output enforcing UTF-8 for emojis
  private def captureOutput(block: => Unit): String = {
    val stream = new ByteArrayOutputStream()
    val printStream = new PrintStream(stream, true, "UTF-8")
    Console.withOut(printStream) {
      block
    }
    stream.toString("UTF-8")
  }

  test("Profiler should NOT output anything when disabled (Zero Overhead)") {
    AwanConfig.enableProfiler = false
    
    val output = captureOutput {
      AwanDBInternalProfiler.start("TEST-DISABLED")
      AwanDBInternalProfiler.stampPhase1()
      AwanDBInternalProfiler.stampPhase2()
      AwanDBInternalProfiler.finish()
    }
    
    output.trim shouldBe empty
  }

  test("Profiler should record and output all phases correctly when enabled") {
    AwanConfig.enableProfiler = true
    
    val output = captureOutput {
      AwanDBInternalProfiler.start("TEST-ENABLED")
      Thread.sleep(10) // Mock Phase 1 (AST Parse)
      AwanDBInternalProfiler.stampPhase1()
      
      Thread.sleep(15) // Mock Phase 2 (JNI Exec)
      AwanDBInternalProfiler.stampPhase2()
      
      Thread.sleep(5)  // Mock Phase 3 (Arrow Packing)
      AwanDBInternalProfiler.finish()
    }

    output should include("🔍 [TEST-ENABLED] Thread-")
    output should include("├─ 🧠 AST Parse:")
    output should include("├─ ⚡ JNI Execution:")
    output should include("└─ 🚀 Arrow Packing:")
    
    // Quick sanity check on timing logic (won't be exact due to thread scheduling, but should be > 0)
    output should not include "AST Parse: 0ms"
  }

  test("Profiler should be strictly thread-safe across concurrent Flight streams") {
    AwanConfig.enableProfiler = true
    
    val numThreads = 10
    val tasks = (1 to numThreads).map { i =>
      Future {
        val reqId = s"THREAD-TEST-$i"
        
        // Console.withOut uses DynamicVariable (ThreadLocal), safely isolating output per Future
        val output = captureOutput {
          AwanDBInternalProfiler.start(reqId)
          Thread.sleep(10) 
          AwanDBInternalProfiler.stampPhase1()
          Thread.sleep(10)
          AwanDBInternalProfiler.stampPhase2()
          Thread.sleep(10)
          AwanDBInternalProfiler.finish()
        }
        (reqId, output)
      }
    }

    val results = Await.result(Future.sequence(tasks), 5.seconds)
    
    results.foreach { case (expectedReqId, output) =>
      output should include(s"🔍 [$expectedReqId]")
      output should include("├─ 🧠 AST Parse:")
      output should include("├─ ⚡ JNI Execution:")
      output should include("└─ 🚀 Arrow Packing:")
    }
  }

  test("Profiler should handle skipped phases gracefully (e.g. on early exception)") {
    AwanConfig.enableProfiler = true
    
    val output = captureOutput {
      AwanDBInternalProfiler.start("TEST-SKIP")
      // Imagine an exception happens here, skipping Phase 1 and 2
      AwanDBInternalProfiler.finish()
    }

    output should include("🔍 [TEST-SKIP] Thread-")
    // Should default to 0ms or similar without crashing
    output should include("├─ 🧠 AST Parse: 0ms") 
    output should include("├─ ⚡ JNI Execution: 0ms")
  }
}