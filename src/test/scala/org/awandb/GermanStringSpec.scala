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

import org.awandb.core.jni.NativeBridge
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GermanStringSpec extends AnyFlatSpec with Matchers {

  "AwanDB German String Engine" should "filter text using AVX2 Prefix Optimization" in {
    val rowCount = 100000
    val targetShort = "AwanDB"
    val targetLong = "AwanDB_High_Performance_Engine_2026"
    
    // 1. Prepare Data
    val data = new Array[String](rowCount)
    for (i <- 0 until rowCount) {
      if (i % 100 == 0) data(i) = targetShort       // 1% Short Match
      else if (i % 100 == 1) data(i) = targetLong   // 1% Long Match
      else data(i) = s"Row_$i"                      // Noise
    }

    // 2. Allocate Block (Hybrid Mode) - CRITICAL FIX HERE
    // createBlock(rows, cols) assumes 4-byte integers.
    // GermanStrings are 16 bytes.
    // rowCount * 4  = Exactly enough bytes for the Structs (1.6MB).
    // rowCount * 16 = Enough for Structs (1.6MB) + 4.8MB String Heap.
    val blockPtr = NativeBridge.createBlock(rowCount * 16, 1) 
    
    // 3. Ingest Strings (Native)
    val colIdx = 0
    NativeBridge.loadStringData(blockPtr, colIdx, data)

    // 4. Query: Short String (Inline Prefix Match)
    val startShort = System.nanoTime()
    val countShort = NativeBridge.avxScanString(blockPtr, colIdx, targetShort)
    val durShort = (System.nanoTime() - startShort) / 1e6
    
    println(f"Search 'AwanDB' (Short): $countShort matches in $durShort%.2f ms")
    countShort shouldBe 1000

    // 5. Query: Long String (Prefix Match + Pointer Chase)
    val startLong = System.nanoTime()
    val countLong = NativeBridge.avxScanString(blockPtr, colIdx, targetLong)
    val durLong = (System.nanoTime() - startLong) / 1e6
    
    println(f"Search Long String: $countLong matches in $durLong%.2f ms")
    countLong shouldBe 1000

    // 6. Query: No Match (Fast Reject)
    val countNone = NativeBridge.avxScanString(blockPtr, colIdx, "OracleDB")
    countNone shouldBe 0

    NativeBridge.freeMainStore(blockPtr)
  }
}