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

#pragma once

// [CRITICAL WINDOWS FIXES]
#define _CRT_SECURE_NO_WARNINGS
#define NOMINMAX

#include <jni.h>
#include <immintrin.h> 
#include <nmmintrin.h> 
#include <vector>
#include <cstdio>      
#include <cstring>     
#include <cstdlib>     
#include <new>         
#include <limits>      
#include <thread>
#include <cmath>       

#ifdef _WIN32
    #include <windows.h>
    #include <malloc.h>
#else
    #include <unistd.h>
#endif

#include "block.h"     
#include "cuckoo.h"    

// Shared Memory Allocators
void* alloc_aligned(size_t size);
void free_aligned(void* ptr);