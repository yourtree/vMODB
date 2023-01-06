package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;

import java.lang.reflect.Method;

/**
 * A data class that stores the method, respective class,
 * and the name of the inbound events that trigger this method.
 * -
 * This method can be reused so the scheduler can keep track
 * of the missing events.
 * -
 * THis is part of a global cross-microservice transaction
 * -
 * Method
 * <a href="https://stackoverflow.com/questions/4685563/how-to-pass-a-function-as-a-parameter-in-java">...</a>
 */
public record VmsTransactionSignature (

    // the instance of a class annotated with @Microservice
    Object vmsInstance,
    // the method to run, i.e., the sub-transaction to be called
    Method method,
    // the type of the transaction, R, W, RW
    TransactionTypeEnum type,
    // the identification of the input queues. i.e., these events must have arrived in order to execute the method
    String[] inputQueues,
    String outputQueue
){}