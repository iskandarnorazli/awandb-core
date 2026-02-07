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
import scala.util.Random

class DictionarySpec extends AnyFlatSpec with Matchers {

  val ROWS = 1_000_000
  
  "AwanDB Dictionary Engine" should "compress Strings to Ints and enable fast Sorting" in {
    
    // 1. DATA GEN (Simulating a High-Traffic Column like "Country" or "Status")
    val distinctValues = Array("USA", "Malaysia", "China", "India", "UK", "Germany", "Japan", "Brazil")
    val rawStrings = Array.fill(ROWS)(distinctValues(Random.nextInt(distinctValues.length)))
    
    // 2. ENCODE (Compress to Ints)
    val dictPtr = NativeBridge.dictionaryCreate()
    val encodedPtr = NativeBridge.allocMainStore(ROWS)
    
    val t0 = System.nanoTime()
    NativeBridge.dictionaryEncodeBatch(dictPtr, rawStrings, encodedPtr)
    val encodeTime = (System.nanoTime() - t0) / 1e6
    println(s"Batch Encoding Time: $encodeTime ms")

    // 3. VERIFY CORRECTNESS
    // Check if the first encoded ID decodes back to the original string
    val checkBuf = new Array[Int](1)
    NativeBridge.copyToScala(encodedPtr, checkBuf, 1)
    val decodedStr = NativeBridge.dictionaryDecode(dictPtr, checkBuf(0))
    decodedStr shouldBe rawStrings(0)

    // 4. BENCHMARK: String Sort vs Encoded Radix Sort
    
    // A. AwanDB Encoded Sort (Radix Sort on Ints)
    val t1 = System.nanoTime()
    NativeBridge.radixSort(encodedPtr, ROWS)
    val radixTime = (System.nanoTime() - t1) / 1e6
    
    // B. Java String Sort (Baseline)
    val t2 = System.nanoTime()
    java.util.Arrays.sort(rawStrings.clone().asInstanceOf[Array[Object]])
    val javaTime = (System.nanoTime() - t2) / 1e6
    
    println(s"\n=======================================================================")
    println(s"| %-20s | %-20s | %-10s |".format("JAVA STRING SORT", "ENCODED RADIX SORT", "SPEEDUP"))
    println(s"=======================================================================")
    println(s"| %17.2f ms | %17.2f ms | %9.2fx |".format(javaTime, radixTime, javaTime / radixTime))
    println(s"=======================================================================\n")

    // Cleanup
    NativeBridge.freeMainStore(encodedPtr)
    NativeBridge.dictionaryDestroy(dictPtr)
  }
}