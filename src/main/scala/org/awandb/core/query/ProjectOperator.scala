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

package org.awandb.core.query

import org.awandb.core.jni.NativeBridge

/**
 * ProjectOperator applies row-level scalar functions (e.g., UPPER, ROUND) 
 * directly to a VectorBatch in-place using Native C++ SIMD/Scalar transformations.
 */
class ProjectOperator(
    child: Operator, 
    val funcName: String
) extends Operator {

  override def open(): Unit = child.open()

  override def next(): VectorBatch = {
    val batch = child.next()
    
    // Safely return null if child is exhausted
    if (batch == null || batch.count == 0) return null

    // Hand the native memory pointer to C++ to mutate the values IN-PLACE
    NativeBridge.applyScalar(
      batch.valuesPtr, 
      batch.count, 
      batch.valueWidthBytes, 
      funcName.toUpperCase
    )

    batch
  }

  override def close(): Unit = child.close()
}