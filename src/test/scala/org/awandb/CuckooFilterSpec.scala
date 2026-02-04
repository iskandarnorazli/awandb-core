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

package org.awandb.core.index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Random

class CuckooFilterSpec extends AnyFlatSpec with Matchers {

  "CuckooFilter" should "accurately store and retrieve items" in {
    val filter = new CuckooFilter(10000)
    
    // Insert
    filter.insert(42) should be (true)
    filter.insert(999) should be (true)
    
    // Check positive
    filter.contains(42) should be (true)
    filter.contains(999) should be (true)
    
    // Check negative
    filter.contains(123) should be (false)
    
    filter.close()
  }

  it should "handle batch loading efficiently" in {
    val count = 100000
    val data = Array.fill(count)(Random.nextInt())
    
    // Sizing factor 1.1x to prevent kicking loops
    val filter = new CuckooFilter((count * 1.1).toInt)
    
    val start = System.nanoTime()
    
    // [FIX] Updated method name from 'load' to 'insertBatch'
    filter.insertBatch(data)
    
    val dur = (System.nanoTime() - start) / 1e6
    println(f"Cuckoo Build Time ($count items): $dur%.2f ms")
    
    // Verification
    var hits = 0
    for(x <- data.take(1000)) {
        if(filter.contains(x)) hits += 1
    }
    hits should be (1000) // Must find all inserted items
    
    filter.close()
  }
  
  it should "maintain a low false positive rate" in {
    val capacity = 10000
    val filter = new CuckooFilter(capacity)
    
    // Insert evens
    for (i <- 0 until capacity by 2) filter.insert(i)
    
    // Check odds (should NOT be there)
    var falsePositives = 0
    var totalChecks = 0
    
    for (i <- 1 until capacity by 2) {
        if (filter.contains(i)) falsePositives += 1
        totalChecks += 1
    }
    
    val rate = falsePositives.toDouble / totalChecks
    println(f"False Positive Rate: ${rate * 100}%.4f%%")
    
    // With 16-bit fingerprints, expected FPR is ~0.02%
    // We set a safe upper bound for the test
    rate should be < 0.01 
    
    filter.close()
  }
}