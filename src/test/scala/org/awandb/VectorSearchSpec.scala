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

package org.awandb.core.engine

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.File
import scala.util.Random

class VectorSearchSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var table: AwanTable = _
  val testDir = "target/data_vector"

  override def beforeEach(): Unit = {
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    else dir.mkdirs()
    
    // 1000 rows per block
    table = new AwanTable("test_vector", 1000, testDir) 
    table.addColumn("id")
    table.addColumn("embedding", isVector = true)
  }

  override def afterEach(): Unit = {
    table.close()
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
  }

  test("Vector Engine should perform native Cosine Similarity (Happy Path)") {
    table.insertRow(Array(1, Array(1.0f, 0.0f, 0.0f)))
    table.insertRow(Array(2, Array(0.0f, 1.0f, 0.0f)))
    table.insertRow(Array(3, Array(0.9f, 0.1f, 0.0f)))
    table.flush() 

    val results = table.queryVector("embedding", Array(1.0f, 0.0f, 0.0f), 0.8f)

    results.length shouldBe 2
    results should contain (1)
    results should contain (3)
  }

  test("Vector Engine MUST respect Row Deletions (Tombstones)") {
    table.insertRow(Array(1, Array(1.0f, 0.0f, 0.0f))) // Perfect match
    table.insertRow(Array(2, Array(0.85f, 0.15f, 0.0f))) // Good match
    table.insertRow(Array(3, Array(0.0f, 1.0f, 0.0f))) // No match
    table.flush() 

    // Delete the perfect match!
    table.delete(1)

    // Query for X-axis. It should only return ID 2 now.
    val results = table.queryVector("embedding", Array(1.0f, 0.0f, 0.0f), 0.8f)

    results.length shouldBe 1
    results should contain (2)
    results should not contain (1) // Proves the C++ AVX loop respects the deletion bitmask
  }

  test("Vector Engine should safely scan across multiple memory blocks") {
    // Insert 2500 rows (Spans 3 full blocks: 1000, 1000, 500)
    for (i <- 1 to 2500) {
      if (i == 1500) {
        // Plant a perfect match in the middle of Block 2
        table.insertRow(Array(i, Array(1.0f, 1.0f, 1.0f)))
      } else {
        // Garbage vectors
        table.insertRow(Array(i, Array(0.0f, 0.0f, 0.0f)))
      }
      
      // Force block creation every 1000 rows
      if (i % 1000 == 0) table.flush() 
    }
    table.flush() // Flush the remaining 500

    table.blockManager.getLoadedBlocks.size shouldBe 3

    val results = table.queryVector("embedding", Array(1.0f, 1.0f, 1.0f), 0.99f)

    results.length shouldBe 1
    results should contain (1500)
  }

  test("Vector Engine should handle high-dimensional embeddings (128D)") {
    val dim = 128
    
    // Create mathematically distinct (orthogonal) vectors
    // Target: [1.0, 0.0, 0.0, ...]
    val targetVector = new Array[Float](dim)
    targetVector(0) = 1.0f
    
    // Ortho 1: [0.0, 1.0, 0.0, ...]
    val ortho1 = new Array[Float](dim)
    ortho1(1) = 1.0f
    
    // Ortho 2: [0.0, 0.0, 1.0, ...]
    val ortho2 = new Array[Float](dim)
    ortho2(2) = 1.0f

    table.insertRow(Array(10, targetVector))
    table.insertRow(Array(20, ortho1))
    table.insertRow(Array(30, ortho2))
    table.flush()

    // Query for the target vector. Threshold 0.99 ensures exact matches only.
    val results = table.queryVector("embedding", targetVector, 0.99f)

    // With true orthogonality, Cosine Similarity is 0.0. Only ID 10 can possibly match.
    results.length shouldBe 1
    results should contain (10)
  }
  
  test("Vector Engine should strictly enforce the similarity threshold boundaries") {
    // We use known geometric angles to test precise cosine values
    // Target: [1.0, 0.0, 0.0]
    table.insertRow(Array(1, Array(1.0f, 0.0f, 0.0f))) 
    
    // Angle 30 degrees -> Cosine similarity is ~0.866
    table.insertRow(Array(2, Array(0.866f, 0.5f, 0.0f))) 
    
    // Angle 60 degrees -> Cosine similarity is exactly 0.5
    table.insertRow(Array(3, Array(0.5f, 0.866f, 0.0f))) 
    table.flush()

    // Query with threshold 0.8 -> Should catch ID 1 (1.0) and ID 2 (0.866), but drop ID 3 (0.5)
    val results = table.queryVector("embedding", Array(1.0f, 0.0f, 0.0f), 0.8f)

    results.length shouldBe 2
    results should contain (1)
    results should contain (2)
    results should not contain (3)
  }

  test("Vector Engine should correctly handle perfectly opposite vectors (Similarity = -1.0)") {
    table.insertRow(Array(1, Array(1.0f, 1.0f, 1.0f)))
    
    // Exact mathematical opposite
    table.insertRow(Array(2, Array(-1.0f, -1.0f, -1.0f))) 
    table.flush()

    // Querying for the positive vector with a threshold of 0.0
    // The opposite vector yields -1.0, which is less than 0.0, so it must be excluded.
    val results = table.queryVector("embedding", Array(1.0f, 1.0f, 1.0f), 0.0f)
    
    results.length shouldBe 1
    results should contain (1)
    results should not contain (2) 
  }

  test("Vector Engine should not crash or return false positives on zero-magnitude vectors") {
    table.insertRow(Array(1, Array(1.0f, 0.5f, 0.2f)))
    
    // An empty/zero vector (Magnitude = 0)
    table.insertRow(Array(2, Array(0.0f, 0.0f, 0.0f))) 
    table.flush()

    // Querying against the zero vector. 
    // The engine should either skip it or safely assign it a similarity of 0.0 / -1.0, 
    // but it should NEVER pass a high threshold like 0.5.
    val results = table.queryVector("embedding", Array(1.0f, 0.0f, 0.0f), 0.5f)
    
    results.length shouldBe 1
    results should contain (1)
    results should not contain (2)
  }

  test("Vector Engine should return exactly K results ranked by highest similarity") {
    // The query target will be [1.0, 0.0, 0.0]
    
    table.insertRow(Array(1, Array(0.6f, 0.8f, 0.0f)))   // Similarity: 0.6 (4th best)
    table.insertRow(Array(2, Array(1.0f, 0.0f, 0.0f)))   // Similarity: 1.0 (1st best - Perfect)
    table.insertRow(Array(3, Array(0.9f, 0.435f, 0.0f))) // Similarity: ~0.9 (2nd best)
    table.insertRow(Array(4, Array(0.8f, 0.6f, 0.0f)))   // Similarity: 0.8 (3rd best)
    table.flush()

    // Query with a loose threshold of 0.5, but STRICT limit of 2 results
    // Signature expectation: queryVector(column, query, threshold, limit)
    val results = table.queryVector("embedding", Array(1.0f, 0.0f, 0.0f), 0.5f, 2)

    // 1. Exact Number Constraint: It must return exactly 2 items, even though all 4 pass the 0.5 threshold.
    results.length shouldBe 2

    // 2. Ranking Constraint: It must sort them so the highest similarity comes first.
    // Rank 1 should be ID 2 (1.0 similarity)
    // Rank 2 should be ID 3 (~0.9 similarity)
    results(0) shouldBe 2
    results(1) shouldBe 3
  }
}