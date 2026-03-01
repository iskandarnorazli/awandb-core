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

import org.awandb.core.jni.NativeBridge

class GraphTable(val numVertices: Int, val numEdges: Int) {
  
  // 1. Allocate Native Memory for CSR Arrays
  // rowPtrs needs (V + 1) ints. allocMainStore handles the byte-sizing internally.
  val rowPtrsAddr: Long = NativeBridge.allocMainStore(numVertices + 1)
  val colIdxsAddr: Long = NativeBridge.allocMainStore(numEdges)
  
  def loadFromArrays(rowPtrs: Array[Int], colIdxs: Array[Int]): Unit = {
    NativeBridge.loadData(rowPtrsAddr, rowPtrs)
    NativeBridge.loadData(colIdxsAddr, colIdxs)
  }
  
  def bfs(startNode: Int): Array[Int] = {
    // Allocate space for the result distances (one per vertex)
    val distancesAddr = NativeBridge.allocMainStore(numVertices)
    
    // Execute Native BFS!
    NativeBridge.csrBfs(
      rowPtrsAddr, 
      colIdxsAddr, 
      numVertices, 
      startNode, 
      distancesAddr
    )
    
    // Bring results back to Scala
    val distances = new Array[Int](numVertices)
    NativeBridge.copyToScala(distancesAddr, distances, numVertices)
    
    NativeBridge.freeMainStore(distancesAddr)
    distances
  }
  
  def close(): Unit = {
    if (rowPtrsAddr != 0) NativeBridge.freeMainStore(rowPtrsAddr)
    if (colIdxsAddr != 0) NativeBridge.freeMainStore(colIdxsAddr)
  }
}