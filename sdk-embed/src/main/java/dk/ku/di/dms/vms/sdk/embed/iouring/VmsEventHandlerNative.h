#pragma once

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

// ========== IoUringChannelGroup JNI ==========
JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannelGroup_nativeCreate
  (JNIEnv *, jclass, jint, jint);

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannelGroup_nativeDestroy
  (JNIEnv *, jclass, jlong);

// ========== IoUringChannel JNI ===============
JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeCreate
  (JNIEnv *, jclass, jlong);

JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeFromFileDescriptor
  (JNIEnv *, jclass, jint, jlong);

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeDestroy
  (JNIEnv *, jclass, jlong);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeConnect
  (JNIEnv *, jobject, jlong, jstring, jint, jobject);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeAccept
  (JNIEnv *, jobject, jlong, jobject);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeRead
  (JNIEnv *, jobject, jlong, jobject, jlong, jint, jobject);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeWrite
  (JNIEnv *, jobject, jlong, jobject, jlong, jint, jobject);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeClose
  (JNIEnv *, jobject, jlong);

JNIEXPORT jboolean JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeIsOpen
  (JNIEnv *, jobject, jlong);

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeConfigureSocket
  (JNIEnv *, jobject, jlong, jint, jint);

JNIEXPORT jstring JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeGetRemoteAddress
  (JNIEnv *, jobject, jlong);

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeSetJavaChannel
  (JNIEnv *, jobject, jlong, jobject);

#ifdef __cplusplus
}
#endif

