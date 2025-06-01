#ifndef _LIBURING_SOCKET_PROVIDER_DEFINED
#define _LIBURING_SOCKET_PROVIDER_DEFINED

#include <jni.h>
#include <stdint.h>

JNIEXPORT jint JNICALL
Java_dk_ku_di_dms_vms_modb_iouring_AbstractIoUringSocket_create(JNIEnv *env, jclass cls);

JNIEXPORT void JNICALL
Java_dk_ku_di_dms_vms_modb_iouring_IoUringServerSocket_bind(JNIEnv *env, jclass cls, jlong server_socket_fd, jstring host, jint port, jint backlog);

JNIEXPORT void JNICALL
Java_dk_ku_di_dms_vms_modb_iouring_AbstractIoUringSocket_setSocketOption(JNIEnv *env, jobject this, jint optionId, jint value);

JNIEXPORT jint JNICALL
Java_dk_ku_di_dms_vms_modb_iouring_AbstractIoUringSocket_getSocketOption(JNIEnv *env, jobject this, jint optionId);

int32_t throw_buffer_overflow_exception(JNIEnv *env);

#endif
