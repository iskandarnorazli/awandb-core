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

import org.awandb.core.query._

object QueryExecutor {
  
  /**
   * Executes a physical operator plan (DAG).
   * This is the "Driver" of the Volcano Model.
   * * @param rootOp The root of the operator tree (e.g., Sort or Agg)
   * @return Number of rows in the final result
   */
  def execute(rootOp: Operator): Long = {
    var rowsProcessed = 0L
    
    // 1. Initialize the Operator Tree (Recursive)
    rootOp.open()
    
    // 2. Consume Batches (Volcano Loop)
    var batch = rootOp.next()
    while (batch != null) {
      rowsProcessed += batch.count
      
      // [Phase 6 Hook] 
      // In the future Server mode, we will serialize 'batch' to Arrow IPC 
      // and flush it to the network socket here.
      
      // Move to next batch
      batch = rootOp.next()
    }
    
    // 3. Cleanup Resources
    rootOp.close()
    
    rowsProcessed
  }
}