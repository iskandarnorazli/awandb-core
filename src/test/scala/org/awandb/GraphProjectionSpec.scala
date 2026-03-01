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

package org.awandb.core.engine

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.File

class GraphProjectionSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var table: AwanTable = _
  val testDir = "target/data_graph_proj"

  override def beforeEach(): Unit = {
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    else dir.mkdirs()
    
    table = new AwanTable("edges", 1000, testDir)
    table.addColumn("id") // Primary Key
    table.addColumn("src_id") // Graph Edges
    table.addColumn("dst_id")
  }

  override def afterEach(): Unit = {
    table.close()
    val dir = new File(testDir)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
  }

  test("AwanTable should dynamically project RAM and Disk rows into a Native CSR Graph (Happy Path)") {
    // 0 -> 1, 2
    // 1 -> 2, 3
    // 2 -> 3
    // 4 -> 0
    table.insertRow(Array(100, 0, 1))
    table.insertRow(Array(101, 0, 2))
    table.insertRow(Array(102, 1, 2))
    table.insertRow(Array(103, 1, 3))
    
    table.flush() // Push first half to Native Disk
    
    table.insertRow(Array(104, 2, 3))
    table.insertRow(Array(105, 4, 0)) // Keep in RAM Delta Buffer

    // Delete an edge to prove the projection respects tombstones
    table.insertRow(Array(106, 4, 3)) 
    table.delete(106)

    val graph = table.projectToGraph("src_id", "dst_id")

    graph.numVertices shouldBe 5
    graph.numEdges shouldBe 6

    val distances = graph.bfs(startNode = 0)
    
    distances(0) shouldBe 0
    distances(1) shouldBe 1
    distances(2) shouldBe 1
    distances(3) shouldBe 2
    distances(4) shouldBe -1 

    graph.close()
  }

  test("Graph Projection should handle an empty table gracefully") {
    val graph = table.projectToGraph("src_id", "dst_id")
    
    graph.numVertices shouldBe 0
    graph.numEdges shouldBe 0
    
    graph.close()
  }

  test("Graph Projection should handle a table where ALL rows are deleted (100% Tombstoned)") {
    table.insertRow(Array(1, 0, 1))
    table.insertRow(Array(2, 1, 2))
    table.flush()
    
    // Tombstone the entire database
    table.delete(1)
    table.delete(2)
    
    val graph = table.projectToGraph("src_id", "dst_id")
    
    // Should behave exactly like an empty graph without crashing
    graph.numVertices shouldBe 0
    graph.numEdges shouldBe 0
    
    graph.close()
  }

  test("Graph Projection should handle sparse graphs with large ID gaps (Disconnected Components)") {
    // Island A: 0 -> 1
    table.insertRow(Array(1, 0, 1))
    
    // Island B: 99 -> 100
    table.insertRow(Array(2, 99, 100))
    table.flush()

    val graph = table.projectToGraph("src_id", "dst_id")
    
    // CSR arrays require (max_id + 1) slots. Max ID is 100, so numVertices = 101
    graph.numVertices shouldBe 101
    graph.numEdges shouldBe 2

    val distFrom0 = graph.bfs(startNode = 0)
    distFrom0(1) shouldBe 1
    distFrom0(99) shouldBe -1
    distFrom0(100) shouldBe -1

    val distFrom99 = graph.bfs(startNode = 99)
    distFrom99(0) shouldBe -1
    distFrom99(1) shouldBe -1
    distFrom99(100) shouldBe 1

    graph.close()
  }

  test("Graph Projection should reject requests for invalid column names") {
    assertThrows[IllegalArgumentException] {
      table.projectToGraph("bad_col", "dst_id")
    }
    assertThrows[IllegalArgumentException] {
      table.projectToGraph("src_id", "missing_col")
    }
  }
}