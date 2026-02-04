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

class VectorSpec extends AnyFlatSpec with Matchers {

  "AwanDB Vector Engine" should "perform fast Cosine Similarity search" in {
    val rows = 1000
    val dim = 128 // Standard small embedding size
    val vectorSize = rows * dim
    
    // 1. Allocate Block
    // We need space for (rows * dim) floats.
    // createBlock uses 4-byte Ints. Float is also 4 bytes.
    // So createBlock(rows * dim, 1) allocates exactly enough bytes.
    val blockPtr = NativeBridge.createBlock(rows * dim, 1)
    
    // 2. Generate Data (Normalized)
    val data = new Array[Float](vectorSize)
    val query = new Array[Float](dim)
    
    // Fill query with 1.0 (Unit Vector if normalized by 1/sqrt(dim))
    val norm = 1.0f / math.sqrt(dim).toFloat
    for (i <- 0 until dim) query(i) = norm
    
    // Fill Data
    // Row 0: Exact Match (1.0 similarity)
    for (i <- 0 until dim) data(i) = norm
    
    // Row 1: Orthogonal (0.0 similarity)
    for (i <- dim until dim*2) data(i) = 0.0f
    // Trick: Set one dimension negative to ensure it's not a match
    data(dim) = -norm 
    
    // Row 2: Partial Match (~0.5)
    
    // Load
    NativeBridge.loadVectorData(blockPtr, 0, data, dim)
    
    // 3. Search High Threshold (Should find Row 0)
    val countHigh = NativeBridge.avxScanVectorCosine(blockPtr, 0, query, 0.99f)
    println(s"Vector Search (>0.99): Found $countHigh")
    countHigh shouldBe 1 // Row 0
    
    // 4. Search Low Threshold (Should find more if we populated them)
    // For this simple test, we just verifying kernel runs without crashing
    
    NativeBridge.freeMainStore(blockPtr)
  }
}