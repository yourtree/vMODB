#ifndef _LIBURING_FILE_PROVIDER_DEFINED
#define _LIBURING_FILE_PROVIDER_DEFINED

#include <jni.h>

JNIEXPORT jint JNICALL
Java_dk_ku_di_dms_vms_web_1common_iouring_IoUringFile_open(JNIEnv *env, jclass cls, jstring path);

#endif
