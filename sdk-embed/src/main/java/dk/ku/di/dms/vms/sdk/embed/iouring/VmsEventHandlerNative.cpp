#include "VmsEventHandlerNative.h"
#include "IoUringChannel.h"
#include <iostream>

// ---------------- Macros --------------------
#define LOG_ERROR(msg) std::cerr << "[VmsEventHandlerNative ERROR] " << msg << std::endl

// ---------------- Helpers -------------------
namespace {
    template<typename T>
    T* castHandle(jlong h) { return reinterpret_cast<T*>(h); }

    void* directBuf(JNIEnv* env, jobject buf, jint* capOut) {
        void* p = env->GetDirectBufferAddress(buf);
        if (!p) { LOG_ERROR("GetDirectBufferAddress failed"); return nullptr; }
        jlong cap = env->GetDirectBufferCapacity(buf);
        if (cap < 0) { LOG_ERROR("GetDirectBufferCapacity failed"); return nullptr; }
        if (capOut) *capOut = static_cast<jint>(cap);
        return p;
    }
}

// =============================================================
//  IoUringChannelGroup JNI
// =============================================================
JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannelGroup_nativeCreate
  (JNIEnv*, jclass, jint ringEntries, jint threads) {
    auto* grp = IoUringChannelGroup::create(ringEntries, threads);
    return reinterpret_cast<jlong>(grp);
}

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannelGroup_nativeDestroy
  (JNIEnv*, jclass, jlong handle) {
    delete castHandle<IoUringChannelGroup>(handle);
}

// =============================================================
//  IoUringChannel JNI
// =============================================================
JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeCreate
  (JNIEnv*, jclass, jlong grpHandle) {
    auto* grp = castHandle<IoUringChannelGroup>(grpHandle);
    if (!grp) return 0;
    auto* ch  = IoUringChannel::create(grp);
    return reinterpret_cast<jlong>(ch);
}

JNIEXPORT jlong JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeFromFileDescriptor
  (JNIEnv*, jclass, jint fd, jlong grpHandle) {
    auto* grp = castHandle<IoUringChannelGroup>(grpHandle);
    if (!grp) return 0;
    auto* ch  = IoUringChannel::fromFd(fd, grp);
    return reinterpret_cast<jlong>(ch);
}

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeDestroy
  (JNIEnv*, jclass, jlong chHandle) {
    delete castHandle<IoUringChannel>(chHandle);
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeConnect
  (JNIEnv* env, jobject self, jlong chHandle, jstring hostStr, jint port, jobject handler) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    if (!ch) return IoUringChannel::STATUS_ERROR;
    const char* host = env->GetStringUTFChars(hostStr, nullptr);
    if (!host) return IoUringChannel::STATUS_ERROR;
    int res = ch->connect(host, port, self, handler);
    env->ReleaseStringUTFChars(hostStr, host);
    return res;
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeAccept
  (JNIEnv*, jobject self, jlong chHandle, jobject handler) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    return ch ? ch->accept(self, handler) : IoUringChannel::STATUS_ERROR;
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeRead
  (JNIEnv* env, jobject self, jlong chHandle, jobject buf, jlong pos, jint len, jobject handler) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    if (!ch) return IoUringChannel::STATUS_ERROR;
    jint cap{}; void* addr = directBuf(env, buf, &cap); if (!addr) return IoUringChannel::STATUS_ERROR;
    if (len > cap) { LOG_ERROR("read length > capacity"); return IoUringChannel::STATUS_ERROR; }
    return ch->read(addr, pos, static_cast<size_t>(len), self, handler);
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeWrite
  (JNIEnv* env, jobject self, jlong chHandle, jobject buf, jlong pos, jint len, jobject handler) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    if (!ch) return IoUringChannel::STATUS_ERROR;
    jint cap{}; void* addr = directBuf(env, buf, &cap); if (!addr) return IoUringChannel::STATUS_ERROR;
    if (len > cap) { LOG_ERROR("write length > capacity"); return IoUringChannel::STATUS_ERROR; }
    return ch->write(addr, pos, static_cast<size_t>(len), self, handler);
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeClose
  (JNIEnv*, jobject, jlong chHandle) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    return ch ? ch->close() : IoUringChannel::STATUS_ERROR;
}

JNIEXPORT jboolean JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeIsOpen
  (JNIEnv*, jobject, jlong chHandle) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    return (ch && ch->isOpen()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeConfigureSocket
  (JNIEnv*, jobject, jlong chHandle, jint rcv, jint snd) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    return ch ? ch->configureSocket(rcv, snd) : IoUringChannel::STATUS_ERROR;
}

JNIEXPORT jstring JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeGetRemoteAddress
  (JNIEnv* env, jobject, jlong chHandle) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    if (!ch) return nullptr;
    std::string addr = ch->remoteAddress();
    return env->NewStringUTF(addr.c_str());
}

JNIEXPORT void JNICALL Java_dk_ku_di_dms_vms_sdk_embed_iouring_IoUringChannel_nativeSetJavaChannel
  (JNIEnv*, jobject, jlong chHandle, jobject jchan) {
    auto* ch = castHandle<IoUringChannel>(chHandle);
    if (ch) ch->setJavaChannel(jchan);
}