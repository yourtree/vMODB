package dk.ku.di.dms.vms.modb.common.transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used differently depending on the actual deployment
 *
 * If embed, only one instance of this class is used acorss modules
 *
 * If modb-service, then
 *
 */
public class TransactionMetadata {

    // how can I get the transaction id?
    private static final Map<Long,Long> threadToTidMap;

    static {
        threadToTidMap = new HashMap<Long,Long>();
    }

    // a tid might have multiple tasks
    // so a tid can be executed by two different threads
    public static void registerTransaction(long threadId, long tid){
        threadToTidMap.put(threadId, tid);
    }

    public static Long tid(long threadId){
        return threadToTidMap.get(threadId);
    }

}
