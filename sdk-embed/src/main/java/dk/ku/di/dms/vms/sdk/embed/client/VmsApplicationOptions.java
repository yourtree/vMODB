package dk.ku.di.dms.vms.sdk.embed.client;

public record VmsApplicationOptions(String host, int port, String[] packages, int networkBufferSize, int networkThreadPoolSize){}
