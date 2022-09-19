package dk.ku.di.dms.vms.sdk.core.operational;

/**
 * Just a placeholder.
 * The object needs to be converted before being sent
 */
public record OutboundEventResult(long tid, String outputQueue, Object output, boolean terminal) { }