#include "IoUringChannel.h"
#include <netdb.h>
#include <sys/eventfd.h>
#include <netinet/tcp.h>

// --------------- Global JavaVM pointer -----------------------
JavaVM* gJvm = nullptr;

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
    gJvm = vm;
    return JNI_VERSION_1_8; // use 1.8 for widest compatibility
}

// ---------------- Utility macros -----------------------------
#ifdef DEBUG
#  define LOG(msg) std::cerr << "[io_uring] " << msg << std::endl
#else
#  define LOG(msg) (void)0
#endif

// static void throwIf(bool cond, const char* msg) {
//     if (cond) throw std::runtime_error(msg);
// }

// =============================================================
//  IoUringChannelGroup implementation
// =============================================================
IoUringChannelGroup* IoUringChannelGroup::create(int ringEntries, int threads) {
    auto* grp = new IoUringChannelGroup(ringEntries, threads);
    if (!grp->init()) { delete grp; return nullptr; }
    return grp;
}

IoUringChannelGroup::IoUringChannelGroup(int ringEntries, int threads)
    : ringEntries_(ringEntries), threadPoolSize_(threads) {}

IoUringChannelGroup::~IoUringChannelGroup() {
    running_.store(false);
    for (auto& th : threads_) if (th.joinable()) th.join();
    io_uring_queue_exit(&ring_);
}

bool IoUringChannelGroup::init() {
    int ret = io_uring_queue_init(ringEntries_, &ring_, 0);
    if (ret < 0) {
        std::cerr << "io_uring_queue_init failed: " << strerror(-ret) << std::endl;
        return false;
    }

    jvm_ = gJvm;
    if (!jvm_) { std::cerr << "JVM not initialised (JNI_OnLoad missing)" << std::endl; return false; }

    running_.store(true);
    for (int i = 0; i < threadPoolSize_; ++i) threads_.emplace_back(&IoUringChannelGroup::cqThread, this);
    return true;
}

JNIEnv* IoUringChannelGroup::env() {
    if (!jvm_) return nullptr;
    JNIEnv* e = nullptr;
    jint st = jvm_->GetEnv(reinterpret_cast<void**>(&e), JNI_VERSION_1_8);
    if (st == JNI_EDETACHED) {
        JavaVMAttachArgs args{JNI_VERSION_1_8, const_cast<char*>("io_uring-cq"), nullptr};
        if (jvm_->AttachCurrentThread(reinterpret_cast<void**>(&e), &args) != JNI_OK) return nullptr;
    }
    return e;
}

void IoUringChannelGroup::cqThread() {
    JNIEnv* e = env(); if (!e) return;
    while (running_.load()) {
        io_uring_cqe* cqe{};
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret == -EINTR) continue;
        if (ret < 0) { LOG("wait_cqe: " << strerror(-ret)); continue; }

        auto* cb = static_cast<IoUringCallback*>(io_uring_cqe_get_data(cqe));
        if (cb) {
            int res = cqe->res;
            switch (cb->opType) {
                case IoUringCallback::OpType::Connect:
                case IoUringCallback::OpType::Read:
                case IoUringCallback::OpType::Write:
                case IoUringCallback::OpType::Close:
                    // TODO: call back into Java (left as exercise)
                    break;
                case IoUringCallback::OpType::Accept:
                    // build new channel and invoke handler (stubs)
                    if (res >= 0) {
                        auto* child = IoUringChannel::fromFd(res, cb->group);
                        (void)child; // wire into Java side as needed
                    }
                    break;
            }
            // cleanup
            if (cb->jchannel)             e->DeleteGlobalRef(cb->jchannel);
            if (cb->jcompletionHandler)  e->DeleteGlobalRef(cb->jcompletionHandler);
            delete cb->clientAddr; delete cb->clientLen;
            delete cb;
        }
        io_uring_cqe_seen(&ring_, cqe);
    }
    jvm_->DetachCurrentThread();
}

// =============================================================
//  IoUringChannel implementation
// =============================================================
IoUringChannel* IoUringChannel::create(IoUringChannelGroup* grp) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return nullptr; }
    return new IoUringChannel(fd, grp);
}

IoUringChannel* IoUringChannel::fromFd(int fd, IoUringChannelGroup* grp) {
    return new IoUringChannel(fd, grp);
}

IoUringChannel::IoUringChannel(int fd, IoUringChannelGroup* grp)
    : fd_(fd), isOpen_(fd >= 0), group_(grp) {}

IoUringChannel::~IoUringChannel() {
    close();
    if (javaChannel_) {
        if (auto* e = group_->env()) e->DeleteGlobalRef(javaChannel_);
    }
}

int IoUringChannel::connect(const char* host, int port, jobject jchan, jobject handler) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!isOpen_ || isConnected_) return STATUS_ERROR;

    addrinfo hints{}; hints.ai_family=AF_UNSPEC; hints.ai_socktype=SOCK_STREAM;
    addrinfo* res=nullptr;
    std::string portStr = std::to_string(port);
    if (getaddrinfo(host, portStr.c_str(), &hints, &res) != 0) return STATUS_ERROR;

    auto* cb = new IoUringCallback(IoUringCallback::OpType::Connect, fd_);
    cb->group = group_;

    JNIEnv* e = group_->env(); if (!e) { freeaddrinfo(res); delete cb; return STATUS_ERROR; }
    cb->jchannel            = e->NewGlobalRef(jchan);
    cb->jcompletionHandler  = e->NewGlobalRef(handler);

    io_uring_sqe* sqe = io_uring_get_sqe(group_->ring());
    if (!sqe) { freeaddrinfo(res); delete cb; return STATUS_ERROR; }
    io_uring_prep_connect(sqe, fd_, res->ai_addr, res->ai_addrlen);
    io_uring_sqe_set_data(sqe, cb);
    int ret = io_uring_submit(group_->ring());
    freeaddrinfo(res);
    return (ret < 0) ? STATUS_ERROR : STATUS_OK;
}

int IoUringChannel::accept(jobject jchan, jobject handler) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!isOpen_) return STATUS_CLOSED;

    auto* cb = new IoUringCallback(IoUringCallback::OpType::Accept, fd_);
    cb->group = group_;

    cb->clientAddr = new sockaddr_storage;
    cb->clientLen  = new socklen_t(sizeof(sockaddr_storage));

    if (JNIEnv* e = group_->env()) {
        cb->jchannel           = e->NewGlobalRef(jchan);
        cb->jcompletionHandler = e->NewGlobalRef(handler);
    }

    io_uring_sqe* sqe = io_uring_get_sqe(group_->ring());
    if (!sqe) { delete cb->clientAddr; delete cb->clientLen; delete cb; return STATUS_ERROR; }
    io_uring_prep_accept(sqe, fd_, reinterpret_cast<sockaddr*>(cb->clientAddr), cb->clientLen, SOCK_CLOEXEC);
    io_uring_sqe_set_data(sqe, cb);
    int ret = io_uring_submit(group_->ring());
    return (ret < 0) ? STATUS_ERROR : STATUS_OK;
}

int IoUringChannel::read(void* buf, jlong pos, size_t len, jobject jchan, jobject handler) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!isOpen_) return STATUS_CLOSED;

    auto* cb = new IoUringCallback(IoUringCallback::OpType::Read, fd_, buf, len, 0, pos);
    cb->group = group_;
    if (JNIEnv* e = group_->env()) {
        cb->jchannel = e->NewGlobalRef(jchan);
        cb->jcompletionHandler = e->NewGlobalRef(handler);
    }

    io_uring_sqe* sqe = io_uring_get_sqe(group_->ring());
    if (!sqe) { delete cb; return STATUS_ERROR; }
    io_uring_prep_read(sqe, fd_, buf, len, pos);
    io_uring_sqe_set_data(sqe, cb);
    int ret = io_uring_submit(group_->ring());
    return (ret < 0) ? STATUS_ERROR : STATUS_OK;
}

int IoUringChannel::write(void* buf, jlong pos, size_t len, jobject jchan, jobject handler) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!isOpen_) return STATUS_CLOSED;

    auto* cb = new IoUringCallback(IoUringCallback::OpType::Write, fd_, buf, len, 0, pos);
    cb->group = group_;
    if (JNIEnv* e = group_->env()) {
        cb->jchannel = e->NewGlobalRef(jchan);
        cb->jcompletionHandler = e->NewGlobalRef(handler);
    }

    io_uring_sqe* sqe = io_uring_get_sqe(group_->ring());
    if (!sqe) { delete cb; return STATUS_ERROR; }
    io_uring_prep_write(sqe, fd_, buf, len, pos);
    io_uring_sqe_set_data(sqe, cb);
    int ret = io_uring_submit(group_->ring());
    return (ret < 0) ? STATUS_ERROR : STATUS_OK;
}

int IoUringChannel::close() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!isOpen_) return STATUS_CLOSED;

    auto* cb = new IoUringCallback(IoUringCallback::OpType::Close, fd_);
    cb->group = group_;
    io_uring_sqe* sqe = io_uring_get_sqe(group_->ring());
    if (!sqe) { ::close(fd_); isOpen_=false; return STATUS_ERROR; }
    io_uring_prep_close(sqe, fd_);
    io_uring_sqe_set_data(sqe, cb);
    int ret = io_uring_submit(group_->ring());
    if (ret < 0) { ::close(fd_); isOpen_=false; return STATUS_ERROR; }

    isOpen_ = false; isConnected_ = false; return STATUS_OK;
}

int IoUringChannel::configureBlocking(bool blocking) {
    int flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return STATUS_ERROR;
    if (blocking) flags &= ~O_NONBLOCK; else flags |= O_NONBLOCK;
    return (fcntl(fd_, F_SETFL, flags) < 0) ? STATUS_ERROR : STATUS_OK;
}

int IoUringChannel::configureSocket(int rcvBuf, int sndBuf) {
    if (rcvBuf>0 && setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvBuf, sizeof(rcvBuf))<0) return STATUS_ERROR;
    if (sndBuf>0 && setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sndBuf, sizeof(sndBuf))<0) return STATUS_ERROR;
    int flag=1; setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    return STATUS_OK;
}

std::string IoUringChannel::remoteAddress() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (remoteAddr_.empty() && updatePeerInfo()==STATUS_OK) {
        char host[NI_MAXHOST]{}; char serv[NI_MAXSERV]{};
        if (getnameinfo(reinterpret_cast<sockaddr*>(&peerAddr_), peerAddrLen_,
                        host, sizeof(host), serv, sizeof(serv), NI_NUMERICHOST|NI_NUMERICSERV)==0) {
            remoteAddr_ = std::string(host)+":"+serv;
        }
    }
    return remoteAddr_;
}

void IoUringChannel::setJavaChannel(jobject ch) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (JNIEnv* e = group_->env()) {
        if (javaChannel_) e->DeleteGlobalRef(javaChannel_);
        javaChannel_ = e->NewGlobalRef(ch);
    }
}

int IoUringChannel::updatePeerInfo() {
    peerAddrLen_ = sizeof(peerAddr_);
    return (getpeername(fd_, reinterpret_cast<sockaddr*>(&peerAddr_), &peerAddrLen_)<0)
           ? STATUS_ERROR : STATUS_OK;
}