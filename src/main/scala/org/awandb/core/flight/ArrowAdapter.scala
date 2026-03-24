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

package org.awandb.core.flight

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{IntVector, BigIntVector, FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import org.awandb.core.query.VectorBatch
import org.awandb.core.jni.NativeBridge

object ArrowAdapter {

  /**
   * Converts a native C++ VectorBatch directly into an Apache Arrow VectorSchemaRoot
   * bypassing the JVM heap entirely via native memcpy.
   */
  def toVectorSchemaRoot(
      batch: VectorBatch, 
      schema: Schema, 
      allocator: BufferAllocator
  ): VectorSchemaRoot = {
    
    val root = VectorSchemaRoot.create(schema, allocator)
    root.allocateNew()

    if (batch.count > 0) {
      // Calculate how many bytes we need for the validity mask (1 bit per row)
      val validityBytes = (batch.count + 7) / 8
      
      // Create a small array of all 1s (-1 as a signed byte is 11111111 in binary)
      // This tells Arrow that NONE of our data is null.
      val ones = Array.fill[Byte](validityBytes)((-1).toByte)

      // ---------------------------------------------------------
      // 1. ZERO-COPY KEYS (Always Int32 in AwanDB)
      // ---------------------------------------------------------
      val keyVector = root.getVector(0).asInstanceOf[IntVector]
      val keyDestAddr = keyVector.getDataBuffer.memoryAddress()
      val keyBytes = batch.count * 4L
      
      // Blast the C++ data directly into Arrow's Netty Buffer (using your existing memcpy method)
      NativeBridge.memcpy(batch.keysPtr, keyDestAddr, keyBytes)
      
      // Set the validity bits
      keyVector.getValidityBuffer.setBytes(0, ones, 0, validityBytes)
      keyVector.setValueCount(batch.count)

      // ---------------------------------------------------------
      // 2. ZERO-COPY VALUES (Int32 or Int64)
      // ---------------------------------------------------------
      if (schema.getFields.size() > 1) {
        val valVector = root.getVector(1)
        val valDestAddr = valVector.getDataBuffer.memoryAddress()
        val valBytes = batch.count * batch.valueWidthBytes.toLong

        NativeBridge.memcpy(batch.valuesPtr, valDestAddr, valBytes)
        valVector.getValidityBuffer.setBytes(0, ones, 0, validityBytes)
        
        valVector match {
          case iv: IntVector => iv.setValueCount(batch.count)
          case bv: BigIntVector => bv.setValueCount(batch.count)
          case _ => throw new UnsupportedOperationException("Unsupported Arrow value type for current AwanDB Schema")
        }
      }
    }

    root.setRowCount(batch.count)
    root
  }
}