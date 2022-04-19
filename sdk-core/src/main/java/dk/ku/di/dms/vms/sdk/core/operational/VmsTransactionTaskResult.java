package dk.ku.di.dms.vms.sdk.core.operational;

public record VmsTransactionTaskResult(
    int tid,
    int identifier,
    boolean failed){}