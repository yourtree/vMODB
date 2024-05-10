package dk.ku.di.dms.vms.sdk.embed.client;

public final class VmsApplicationOptions {

    private final String host;

    private final int port;

    private final String[] packages;

    private final int networkBufferSize;

    private final int networkThreadPoolSize;

    private final int vmsThreadPoolSize;

    public VmsApplicationOptions(String host, int port, String[] packages, int networkBufferSize, int networkThreadPoolSize, int vmsThreadPoolSize) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.vmsThreadPoolSize = vmsThreadPoolSize;
    }

    public VmsApplicationOptions(String host, int port, String[] packages, int networkBufferSize, int networkThreadPoolSize) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.vmsThreadPoolSize = 0;
    }

    public String host() {
        return host;
    }

    public int networkBufferSize() {
        return networkBufferSize;
    }

    public int networkThreadPoolSize() {
        return networkThreadPoolSize;
    }

    public String[] packages() {
        return packages;
    }

    public int port() {
        return port;
    }

    public int vmsThreadPoolSize() {
        return vmsThreadPoolSize;
    }
}
