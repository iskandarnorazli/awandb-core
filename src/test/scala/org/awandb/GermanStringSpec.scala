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

package org.awandb.specs

import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GermanStringSpec extends AnyFlatSpec with Matchers {

  "AwanDB German String Engine" should "filter text using AVX2 Prefix Optimization" in {
    // [SETUP]
    val rowCount = 100000
    val targetShort = "AwanDB"
    val targetLong = "AwanDB_High_Performance_Engine_2026"
    val noisePrefix = "Row_"

    // 1. Prepare Data (JVM Side)
    val data = new Array[String](rowCount)
    var expectedShort = 0
    var expectedLong = 0

    for (i <- 0 until rowCount) {
      if (i % 100 == 0) {
        data(i) = targetShort
        expectedShort += 1
      } else if (i % 100 == 1) {
        data(i) = targetLong
        expectedLong += 1
      } else {
        data(i) = s"$noisePrefix$i"
      }
    }

    // 2. Allocate Block (Hybrid Mode)
    // CRITICAL MEMORY HACK:
    // createBlock(rows, cols) assumes 4-byte integers.
    // GermanStrings are 16 bytes + Variable Heap.
    // We request 'rowCount * 16' "rows".
    // - 16 bytes (4 ints) go to the Struct.
    // - 48 bytes (12 ints) go to the String Pool (Heap).
    // This gives us 64 bytes per row total, which is plenty for this test.
    val capacityHack = rowCount * 16
    val blockPtr = NativeBridge.createBlock(capacityHack, 1)

    try {
      // 3. Ingest Strings (Native)
      // This pushes data from JVM -> C++ German String Layout
      val colIdx = 0
      NativeBridge.loadStringData(blockPtr, colIdx, data)

      // 4. Query: Short String (Inline Prefix Match)
      // "AwanDB" (6 bytes) fits entirely in the prefix/suffix.
      // The AVX scanner will match it purely on registers.
      val startShort = System.nanoTime()
      val countShort = NativeBridge.avxScanString(blockPtr, colIdx, targetShort)
      val durShort = (System.nanoTime() - startShort) / 1e6

      println(f">> Search 'AwanDB' (Short): $countShort matches in $durShort%.2f ms")
      countShort shouldBe expectedShort

      // 5. Query: Long String (Prefix Match + Pointer Chase)
      // "AwanDB_High..." (>12 bytes) stores prefix locally, body on Heap.
      // AVX checks prefix first, then chases pointer if prefix matches.
      val startLong = System.nanoTime()
      val countLong = NativeBridge.avxScanString(blockPtr, colIdx, targetLong)
      val durLong = (System.nanoTime() - startLong) / 1e6

      println(f">> Search Long String:    $countLong matches in $durLong%.2f ms")
      countLong shouldBe expectedLong

      // 6. Query: No Match (Fast Reject)
      // "OracleDB" prefix differs. Should yield 0 instantly.
      val countNone = NativeBridge.avxScanString(blockPtr, colIdx, "OracleDB")
      countNone shouldBe 0

    } finally {
      // Cleanup
      NativeBridge.freeMainStore(blockPtr)
    }
  }
}