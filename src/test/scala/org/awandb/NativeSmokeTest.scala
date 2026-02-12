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

package org.awandb.core

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.jni.NativeBridge

class NativeSmokeTest extends AnyFunSuite {

  test("00. Native Engine Sanity Check") {
    // 1. This triggers the static block in NativeBridge
    // If the DLL is missing or crashes on load, this line fails.
    println("Attempting to load Native Engine...")
    NativeBridge.init() 
    
    // 2. Ask the C++ engine for CPU info to prove it's alive
    val (cores, cache) = NativeBridge.getHardwareInfo()
    
    println(s"✅ Native Engine Online!")
    println(s"   - Cores: $cores")
    println(s"   - L3 Cache: ${cache / 1024 / 1024} MB")
    
    assert(cores > 0, "C++ returned invalid core count!")
  }
}