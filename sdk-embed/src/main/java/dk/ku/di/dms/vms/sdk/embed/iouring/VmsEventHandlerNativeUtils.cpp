/**
 * VmsEventHandlerNativeUtils.cpp
 * 
 * Additional utility functions for the VmsEventHandler to use io_uring.
 * This file provides helper functions to aid in transitioning from
 * AsynchronousSocketChannel to IoUringChannel.
 */

#include "VmsEventHandlerNative.h"
#include <jni.h>
#include <liburing.h>
#include <string>
#include <iostream>

extern "C" {

/*
 * Checks if io_uring is supported on the current system.
 */
JNIEXPORT jboolean JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeIsIoUringSupported
  (JNIEnv* env, jclass clazz) {
    
    // Try to initialize an io_uring instance
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring, 0);
    
    if (ret < 0) {
        return JNI_FALSE;
    }
    
    // Cleanup
    io_uring_queue_exit(&ring);
    
    return JNI_TRUE;
}

/*
 * Gets the version of liburing installed on the system.
 */
JNIEXPORT jstring JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeGetLibUringVersion
  (JNIEnv* env, jclass clazz) {
    
    unsigned int major = 0;
    unsigned int minor = 0;
    
#ifdef LIBURING_VERSION
    major = LIBURING_VERSION >> 16;
    minor = LIBURING_VERSION & 0xffff;
#endif
    
    std::string version = std::to_string(major) + "." + std::to_string(minor);
    return env->NewStringUTF(version.c_str());
}

/*
 * Returns the native memory address of a direct ByteBuffer.
 * This is useful for transferring buffers between JNI and io_uring.
 */
JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeGetDirectBufferAddress
  (JNIEnv* env, jclass clazz, jobject buffer) {
    
    if (!env->IsSameObject(buffer, NULL)) {
        void* address = env->GetDirectBufferAddress(buffer);
        if (address != NULL) {
            return reinterpret_cast<jlong>(address);
        }
    }
    
    return 0;
}

/*
 * Returns the capacity of a direct ByteBuffer.
 */
JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeGetDirectBufferCapacity
  (JNIEnv* env, jclass clazz, jobject buffer) {
    
    if (!env->IsSameObject(buffer, NULL)) {
        jlong capacity = env->GetDirectBufferCapacity(buffer);
        if (capacity >= 0) {
            return static_cast<jint>(capacity);
        }
    }
    
    return -1;
}

/*
 * Creates a new direct ByteBuffer of the specified capacity.
 */
JNIEXPORT jobject JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeAllocateDirectBuffer
  (JNIEnv* env, jclass clazz, jint capacity) {
    
    void* buffer = malloc(capacity);
    if (buffer == NULL) {
        return NULL;
    }
    
    jobject directBuffer = env->NewDirectByteBuffer(buffer, capacity);
    if (directBuffer == NULL) {
        free(buffer);
        return NULL;
    }
    
    return directBuffer;
}

/*
 * Frees the memory associated with a direct ByteBuffer.
 */
JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringUtils_nativeFreeDirectBuffer
  (JNIEnv* env, jclass clazz, jobject buffer) {
    
    if (!env->IsSameObject(buffer, NULL)) {
        void* address = env->GetDirectBufferAddress(buffer);
        if (address != NULL) {
            free(address);
        }
    }
}

} // extern "C" 