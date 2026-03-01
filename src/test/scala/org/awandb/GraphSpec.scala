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

package org.awandb.core.graph

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GraphSpec extends AnyFunSuite with Matchers {

  test("CSR Graph should perform native BFS traversal (Happy Path)") {
    val numVertices = 5
    val numEdges = 6
    val rowPtrs = Array(0, 2, 4, 5, 5, 6) 
    val colIdxs = Array(1, 2, 2, 3, 3, 0)
    
    val graph = new GraphTable(numVertices, numEdges)
    graph.loadFromArrays(rowPtrs, colIdxs)
    
    val distances = graph.bfs(startNode = 0)
    
    distances(0) shouldBe 0
    distances(1) shouldBe 1
    distances(2) shouldBe 1
    distances(3) shouldBe 2
    distances(4) shouldBe -1 
    
    graph.close()
  }

  test("CSR Graph should safely handle Cycles and Self-Loops without infinite looping") {
    // Node 0 -> 1
    // Node 1 -> 2
    // Node 2 -> 0 (Cycle!)
    // Node 2 -> 2 (Self-Loop!)
    val numVertices = 3
    val numEdges = 4
    
    val rowPtrs = Array(0, 1, 2, 4)
    val colIdxs = Array(1, 2, 0, 2)
    
    val graph = new GraphTable(numVertices, numEdges)
    graph.loadFromArrays(rowPtrs, colIdxs)
    
    // If the cycle detection fails, this will hang forever or crash with OOM.
    val distances = graph.bfs(startNode = 0)
    
    distances(0) shouldBe 0
    distances(1) shouldBe 1
    distances(2) shouldBe 2
    
    graph.close()
  }

  test("CSR Graph should handle disconnected components (Islands)") {
    // Island A: 0 -> 1
    // Island B: 2 -> 3
    val numVertices = 4
    val numEdges = 2
    
    val rowPtrs = Array(0, 1, 1, 2, 2)
    val colIdxs = Array(1, 3)
    
    val graph = new GraphTable(numVertices, numEdges)
    graph.loadFromArrays(rowPtrs, colIdxs)
    
    // Search from Island A
    val distFrom0 = graph.bfs(startNode = 0)
    distFrom0(0) shouldBe 0
    distFrom0(1) shouldBe 1
    distFrom0(2) shouldBe -1 // Island B is unreachable
    distFrom0(3) shouldBe -1
    
    // Search from Island B
    val distFrom2 = graph.bfs(startNode = 2)
    distFrom2(0) shouldBe -1 // Island A is unreachable
    distFrom2(1) shouldBe -1
    distFrom2(2) shouldBe 0
    distFrom2(3) shouldBe 1
    
    graph.close()
  }

  test("CSR Graph should handle deep chains without queue buffer overruns") {
    // 0 -> 1 -> 2 -> ... -> 999
    val numVertices = 1000
    val numEdges = 999
    
    val rowPtrs = new Array[Int](numVertices + 1)
    val colIdxs = new Array[Int](numEdges)
    
    for (i <- 0 until numVertices) {
      rowPtrs(i) = i
      if (i < numEdges) colIdxs(i) = i + 1
    }
    rowPtrs(numVertices) = numEdges // Tail pointer
    
    val graph = new GraphTable(numVertices, numEdges)
    graph.loadFromArrays(rowPtrs, colIdxs)
    
    val distances = graph.bfs(startNode = 0)
    
    distances(0) shouldBe 0
    distances(500) shouldBe 500
    distances(999) shouldBe 999
    
    graph.close()
  }
}