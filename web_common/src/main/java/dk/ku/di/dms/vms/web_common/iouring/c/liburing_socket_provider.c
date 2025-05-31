#include "liburing_socket_provider.h"
#include "liburing_provider.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <liburing.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <errno.h>

#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif

JNIEXPORT jint JNICALL
Java_dk_ku_di_dms_vms_web_1common_iouring_AbstractIoUringSocket_create(JNIEnv *env, jclass cls) {
    int32_t val = 1;

    int32_t fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw_exception(env, "socket", fd);
        return -1;
    }

    int32_t ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int));
    if (ret < 0) {
        throw_exception(env, "setsockopt", ret);
        return -1;
    }

    ret = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
    if (ret == -1) {
        throw_exception(env, "setsockopt", ret);
        return -1;
    }

    return (uint32_t) fd;
}

JNIEXPORT void JNICALL
Java_dk_ku_di_dms_vms_web_1common_iouring_IoUringServerSocket_bind(JNIEnv *env, jclass cls, jlong server_socket_fd, jstring ip_address, jint port, jint backlog) {
    char *ip = (*env)->GetStringUTFChars(env, ip_address, NULL);

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(port);
    srv_addr.sin_addr.s_addr = inet_addr(ip);

    (*env)->ReleaseStringUTFChars(env, ip_address, ip);

    int32_t ret = bind(server_socket_fd, (const struct sockaddr *) &srv_addr, sizeof(srv_addr));
    if (ret < 0) {
        throw_exception(env, "bind", ret);
        return;
    }

    ret = listen(server_socket_fd, backlog);
    if (ret < 0) {
        throw_exception(env, "io_uring_get_sqe", -16);
        return;
    }
}

/**
 * Helper function to get the file descriptor from the Java object
 */
static jint get_fd_from_object(JNIEnv *env, jobject this) {
    jclass clazz = (*env)->GetObjectClass(env, this);
    jfieldID fdField = (*env)->GetFieldID(env, clazz, "fd", "I");
    if (fdField == NULL) {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/NoSuchFieldError"), "Cannot access fd field");
        return -1;
    }
    return (*env)->GetIntField(env, this, fdField);
}

JNIEXPORT void JNICALL
Java_dk_ku_di_dms_vms_web_1common_iouring_AbstractIoUringSocket_setSocketOption(JNIEnv *env, jobject this, jint optionId, jint value) {
    jint fd = get_fd_from_object(env, this);
    if (fd < 0) {
        return; // Exception already thrown
    }

    int ret = 0;
    int sockopt_value = value;
    socklen_t optlen = sizeof(sockopt_value);

    switch (optionId) {
        case 1: // SO_SNDBUF
            ret = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sockopt_value, optlen);
            break;
        case 2: // SO_RCVBUF
            ret = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sockopt_value, optlen);
            break;
        case 3: // SO_KEEPALIVE
            ret = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &sockopt_value, optlen);
            break;
        case 4: // SO_REUSEADDR
            ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sockopt_value, optlen);
            break;
        case 5: // TCP_NODELAY
            ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &sockopt_value, optlen);
            break;
        default:
            (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/IllegalArgumentException"), "Unknown socket option");
            return;
    }

    if (ret < 0) {
        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg), "Failed to set socket option %d: %s", optionId, strerror(errno));
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/io/IOException"), error_msg);
    }
}

JNIEXPORT jint JNICALL
Java_dk_ku_di_dms_vms_web_1common_iouring_AbstractIoUringSocket_getSocketOption(JNIEnv *env, jobject this, jint optionId) {
    jint fd = get_fd_from_object(env, this);
    if (fd < 0) {
        return -1; // Exception already thrown
    }

    int ret = 0;
    int sockopt_value = 0;
    socklen_t optlen = sizeof(sockopt_value);

    switch (optionId) {
        case 1: // SO_SNDBUF
            ret = getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sockopt_value, &optlen);
            break;
        case 2: // SO_RCVBUF
            ret = getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sockopt_value, &optlen);
            break;
        case 3: // SO_KEEPALIVE
            ret = getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &sockopt_value, &optlen);
            break;
        case 4: // SO_REUSEADDR
            ret = getsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sockopt_value, &optlen);
            break;
        case 5: // TCP_NODELAY
            ret = getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &sockopt_value, &optlen);
            break;
        default:
            (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/IllegalArgumentException"), "Unknown socket option");
            return -1;
    }

    if (ret < 0) {
        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg), "Failed to get socket option %d: %s", optionId, strerror(errno));
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/io/IOException"), error_msg);
        return -1;
    }

    return sockopt_value;
}
