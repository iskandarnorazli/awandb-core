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

class HashSpec extends AnyFlatSpec with Matchers {

  "AwanDB Hashing Engine" should "generate stable signatures for Vectors" in {
    val dim = 128
    val rows = 4
    // TO THIS:
    val colSizes = Array(rows * dim * 4) // 4 bytes per Float
    val blockPtr = NativeBridge.createBlock(rows, 1, colSizes) // Pass 'rows' as the true row count!
    
    val data = new Array[Float](rows * dim)
    
    // Row 0: Vector A
    for (i <- 0 until dim) data(i) = 1.0f
    
    // Row 1: Vector B (Different)
    for (i <- dim until dim*2) data(i) = 0.5f
    
    // Row 2: Vector A (Duplicate of Row 0) -> Should have SAME hash
    for (i <- dim*2 until dim*3) data(i) = 1.0f
    
    // Row 3: Vector A' (Slight difference) -> Should have DIFF hash
    for (i <- dim*3 until dim*4) data(i) = 1.0f
    data(dim*3) = 1.00001f // Tiny change
    
    NativeBridge.loadVectorData(blockPtr, 0, data, dim)
    
    // Compute Hashes
    val hashPtr = NativeBridge.computeHashNativePtr(blockPtr, 0)
    val hashes = NativeBridge.getHashes(hashPtr, rows)
    
    println(s"Vector A Hash: ${hashes(0).toHexString}")
    println(s"Vector B Hash: ${hashes(1).toHexString}")
    println(s"Vector A Copy: ${hashes(2).toHexString}")
    println(s"Vector A'Mod: ${hashes(3).toHexString}")
    
    // Assertions
    hashes(0) shouldBe hashes(2)      // Deterministic
    hashes(0) should not be hashes(1) // Discriminative
    hashes(0) should not be hashes(3) // Sensitive to float changes
    
    NativeBridge.freeMainStore(blockPtr)
    NativeBridge.freeMainStore(hashPtr)
  }
}