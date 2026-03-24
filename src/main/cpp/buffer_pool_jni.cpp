/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include <jni.h>
#include "buffer_pool.h"

// A global instance specifically for our Scala tests
static BufferPool* test_pool = nullptr;

extern "C" {

// Adjust the package name in the function names to match your actual Scala package!
// Assuming: package org.awandb.core.jni; object NativeBridge { ... }

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_initTestBufferPool(JNIEnv* env, jobject, jlong capacity) {
    // 1. Get the global Singleton instance instead of using 'new'
    test_pool = BufferPool::get_instance();
    
    // 2. Initialize it with the test capacity
    test_pool->init(static_cast<size_t>(capacity));
}

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_requestTestPage(JNIEnv* env, jobject, jint page_id) {
    if (test_pool) test_pool->request_page(page_id);
}

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_pinTestPage(JNIEnv* env, jobject, jint page_id) {
    // We didn't add a direct pin method to BufferPool, so we'll access the map directly for the test harness,
    // or you can add a pin_page(id) method to BufferPool.h! For now, we simulate a query pinning it:
    if (test_pool && test_pool->is_resident(page_id)) {
        // Note: For a real test harness, add a get_block(id) to BufferPool to do this cleanly.
        // Assuming you add `BlockHeader* get_block(int page_id)` to BufferPool.h:
        test_pool->request_page(page_id)->pin_count++; 
    }
}

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_unpinTestPage(JNIEnv* env, jobject, jint page_id) {
    if (test_pool && test_pool->is_resident(page_id)) {
        test_pool->request_page(page_id)->pin_count--;
    }
}

JNIEXPORT jboolean JNICALL Java_org_awandb_core_jni_NativeBridge_isPageResident(JNIEnv* env, jobject, jint page_id) {
    return test_pool ? test_pool->is_resident(page_id) : false;
}

JNIEXPORT jint JNICALL Java_org_awandb_core_jni_NativeBridge_getBufferPoolUsagePercent(JNIEnv* env, jobject) {
    return test_pool ? test_pool->get_usage_percent() : 0;
}

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_triggerPacemakerSweep(JNIEnv* env, jobject) {
    if (test_pool) test_pool->trigger_pacemaker_sweep();
}

JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_destroyTestBufferPool(JNIEnv* env, jobject) {
    if (test_pool) {
        // Clear the memory tracking safely instead of calling 'delete'
        test_pool->destroy();
        test_pool = nullptr;
    }
}

} // extern "C"