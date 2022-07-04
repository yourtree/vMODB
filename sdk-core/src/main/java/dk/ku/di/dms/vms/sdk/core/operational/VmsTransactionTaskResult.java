package dk.ku.di.dms.vms.sdk.core.operational;

/**
 * Placeholder so the scheduler can identify whether a task has failed
 */
public record VmsTransactionTaskResult(
    long tid,
    int identifier,
    boolean failed){}