package dk.ku.di.dms.vms.modb.common.transaction;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * This class is used differently depending on the actual deployment
 * If embed, only one instance of this class is used across modules
 * If modb-service, then we need another mapping scheme.
 *
 */
public class TransactionMetadata {

    /**
     * Tuple: <tid, identifier>
     */
    private static final Map<Long, TransactionId> threadToTidMap;

    private static volatile TransactionId lastWriteTaskFinished;

    private static final Map<Long, TransactionId> threadToPreviousTransactionMap;

    static {
        threadToTidMap = new HashMap<>();
        threadToPreviousTransactionMap = new HashMap<>();
    }

    // a tid might have multiple tasks
    // so a tid can be executed by two different threads
    public static void registerWriteTaskFinished(long tid, int identifier){
        lastWriteTaskFinished = new TransactionId(tid, identifier);
    }

    public static void registerTransaction(long threadId, long tid, int identifier){
        threadToTidMap.put(threadId, new TransactionId(tid, identifier));
        threadToPreviousTransactionMap.put(threadId, lastWriteTaskFinished);
    }

    public static TransactionId tid(long threadId){
        return threadToTidMap.get(threadId);
    }

    public static TransactionId getPreviousWriteTransaction(long threadId){
        return threadToPreviousTransactionMap.get(threadId);
    }

}
