#include <jni.h>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <memory>

#include "query_context.h"
// [CRITICAL FIX] Cross-Platform Heap Trimming Headers
#if defined(__linux__) || defined(_WIN32) || defined(_WIN64)
#include <malloc.h>
#endif

// Global registry mapping Query IDs to their respective C++ QueryContext
static std::unordered_map<std::string, std::unique_ptr<QueryContext>> active_queries;

// A Read-Write lock: allows concurrent lookups during Morsel execution,
// but exclusive access during query init/destroy.
static std::shared_mutex query_registry_mutex;

extern "C"
{

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_initQueryContextNative(JNIEnv *env, jobject obj, jstring queryId)
    {
        // Extract the Query ID string from the JVM
        const char *qid_chars = env->GetStringUTFChars(queryId, nullptr);
        std::string qid(qid_chars);
        env->ReleaseStringUTFChars(queryId, qid_chars);

        // Lock the registry exclusively to prevent race conditions
        std::unique_lock<std::shared_mutex> lock(query_registry_mutex);

        // Initialize a new QueryContext and add it to the registry map
        active_queries[qid] = std::make_unique<QueryContext>();
    }

    JNIEXPORT void JNICALL Java_org_awandb_core_jni_NativeBridge_destroyQueryContextNative(JNIEnv *env, jobject obj, jstring queryId)
    {
        const char *qid_chars = env->GetStringUTFChars(queryId, nullptr);
        std::string qid(qid_chars);
        env->ReleaseStringUTFChars(queryId, qid_chars);

        std::unique_lock<std::shared_mutex> lock(query_registry_mutex);
        active_queries.erase(qid);

// [CRITICAL FIX] Force the OS to instantly reclaim the 6GB arena!
#ifdef __linux__
        malloc_trim(0); 
#elif defined(_WIN32) || defined(_WIN64)
        _heapmin(); // Windows UCRT equivalent
#endif
    }

    // Helper function to be called by your C++ execution nodes
    QueryContext *getQueryContext(const std::string &qid)
    {
        // Use a shared lock for high-concurrency read access
        std::shared_lock<std::shared_mutex> lock(query_registry_mutex);

        auto it = active_queries.find(qid);
        if (it != active_queries.end())
        {
            return it->second.get();
        }

        // Context not found (either destroyed or never initialized)
        return nullptr;
    }

} // end extern "C"