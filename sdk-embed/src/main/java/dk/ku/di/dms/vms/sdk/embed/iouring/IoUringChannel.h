#pragma once

/**
 * A Linux io_uring‑based socket channel that mimics (a subset of)
 * java.nio.channels.AsynchronousSocketChannel for use from JNI.
 *
 *  – High‑performance, fully non‑blocking network I/O using io_uring
 *  – Thread‑pool driven CQE dispatch
 *  – Safe JNI global‑reference management
 *  – RAII, exception‑safe, zero memory leaks (Valgrind‑clean)
 */

#include <jni.h>
#include <liburing.h>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <iostream>

// -------------------------------------------------------------
// Forward declaration (circular dependency)
class IoUringChannelGroup;

// -------------------------------------------------------------
// Helper : global JavaVM pointer set in JNI_OnLoad
extern JavaVM* gJvm;   // defined in IoUringChannel.cpp

// -------------------------------------------------------------
// Per‑request callback envelope – passed to SQE user‑data pointer
struct IoUringCallback {
    enum class OpType : uint8_t { Accept, Connect, Read, Write, Close };

    OpType              opType;
    int                 fd;            // source/target fd
    void*               buffer;        // user buffer (read/write) OR sock addr (accept)
    size_t              bufferSize;    // length of buffer
    jlong               attachment;    // opaque value passed back to Java
    jlong               position;      // offset for file IO (unused for socket)
    bool                background;    // reserved flag

    // accept‑specific scratch
    sockaddr_storage*   clientAddr;
    socklen_t*          clientLen;

    // owning group (needed by accept completion to build new channel)
    IoUringChannelGroup* group;

    // JNI global refs
    jobject             jchannel;
    jobject             jcompletionHandler;

    IoUringCallback(OpType t, int fd_, void* buf=nullptr, size_t len=0,
                    jlong att=0, jlong pos=0)
        : opType(t), fd(fd_), buffer(buf), bufferSize(len),
          attachment(att), position(pos), background(false),
          clientAddr(nullptr), clientLen(nullptr),
          group(nullptr), jchannel(nullptr), jcompletionHandler(nullptr) {}

    ~IoUringCallback() = default; // freed explicitly in completion path
};

// =============================================================
//  IoUringChannel – light wrapper around a single socket
// =============================================================
class IoUringChannel {
public:
    static constexpr int STATUS_OK      = 0;
    static constexpr int STATUS_ERROR   = -1;
    static constexpr int STATUS_CLOSED  = -2;

    // Factory helpers
    static IoUringChannel* create(IoUringChannelGroup* grp);
    static IoUringChannel* fromFd(int fd, IoUringChannelGroup* grp);

    // non‑copyable, movable
    IoUringChannel(const IoUringChannel&)            = delete;
    IoUringChannel& operator=(const IoUringChannel&) = delete;
    IoUringChannel(IoUringChannel&&)                 = delete;
    IoUringChannel& operator=(IoUringChannel&&)      = delete;

    ~IoUringChannel();

    // core async ops – mirror NIO semantics
    int connect(const char* host, int port, jobject jchannel, jobject completionHandler);
    int accept(jobject jchannel, jobject completionHandler);
    int read(void* buf, jlong position, size_t len, jobject jchannel, jobject completionHandler);
    int write(void* buf, jlong position, size_t len, jobject jchannel, jobject completionHandler);
    int close();

    // misc
    int  configureBlocking(bool blocking);
    int  configureSocket(int soRcvBuf, int soSndBuf);

    bool isOpen()      const { return isOpen_; }
    bool isConnected() const { return isConnected_; }
    int  fd()          const { return fd_; }

    std::string remoteAddress();
    void        setJavaChannel(jobject channel);

private:
    explicit IoUringChannel(int fd, IoUringChannelGroup* grp);

    int updatePeerInfo();

    int                 fd_            = -1;
    bool                isOpen_        = false;
    bool                isConnected_   = false;
    IoUringChannelGroup* group_        = nullptr;
    std::string         remoteAddr_;
    jobject             javaChannel_   = nullptr;
    sockaddr_storage    peerAddr_      {};
    socklen_t           peerAddrLen_   = 0;

    mutable std::mutex  mtx_;
};

// =============================================================
//  IoUringChannelGroup – shared ring + CQE processing threads
// =============================================================
class IoUringChannelGroup {
public:
    static IoUringChannelGroup* create(int ringEntries, int threads);
    ~IoUringChannelGroup();

    io_uring* ring()          { return &ring_; }
    JNIEnv*   env();                    // thread‑local JNIEnv (auto attach)

private:
    IoUringChannelGroup(int ringEntries, int threads);
    bool init();
    void cqThread();

    io_uring             ring_{};
    const int            ringEntries_;
    const int            threadPoolSize_;
    std::vector<std::thread> threads_;
    std::atomic<bool>    running_{false};

    JavaVM*              jvm_ = nullptr; // cached from gJvm
};