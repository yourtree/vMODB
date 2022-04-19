package dk.ku.di.dms.vms.sdk.core.operational;

import java.lang.reflect.Method;

/**
 * A data class that stores the method, respective class,
 * and the name of the inbound events that trigger this method.
 *
 * This method can be reused so the scheduler can keep track
 * of the missing events.
 *
 * THis is part of a global cross-microservice transaction
 */
public record VmsTransactionSignature (
    Object vmsInstance, // a class annotated with @Microservice
    // https://stackoverflow.com/questions/4685563/how-to-pass-a-function-as-a-parameter-in-java
    Method method,
    String[] inputQueues,
    String outputQueue){}