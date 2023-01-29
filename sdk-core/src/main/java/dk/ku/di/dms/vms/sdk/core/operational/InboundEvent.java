package dk.ku.di.dms.vms.sdk.core.operational;

public record InboundEvent (
        long tid, long lastTid, long batch, String event, Class<?> clazz, Object input)
{ }
