package dk.ku.di.dms.vms.sdk.embed.client;

public final class VmsApplicationOptions {

    private final String host;

    private final int port;

    private final String[] packages;

    private final int networkBufferSize;

    private final int networkThreadPoolSize;

    private final int vmsThreadPoolSize;

    private final int networkSendTimeout;

    public VmsApplicationOptions(String host, int port, String[] packages, int networkBufferSize, int networkThreadPoolSize, int vmsThreadPoolSize, int networkSendTimeout) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.vmsThreadPoolSize = vmsThreadPoolSize;
        this.networkSendTimeout = networkSendTimeout;
    }

    public VmsApplicationOptions(String host, int port, String[] packages, int networkBufferSize, int networkThreadPoolSize, int networkSendTimeout) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkSendTimeout = networkSendTimeout;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.vmsThreadPoolSize = 0;
    }

    public String host() {
        return this.host;
    }

    public int networkBufferSize() {
        return this.networkBufferSize;
    }

    public int networkThreadPoolSize() {
        return this.networkThreadPoolSize;
    }

    public int networkSendTimeout(){
        return this.networkSendTimeout;
    }

    public String[] packages() {
        return this.packages;
    }

    public int port() {
        return this.port;
    }

    public int vmsThreadPoolSize() {
        return this.vmsThreadPoolSize;
    }
}
