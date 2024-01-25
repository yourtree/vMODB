package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.transaction.TransactionManager;

/**
 * Cache key objects for reuse across queries
 * Inspiration from JVM class: {jdk.internal.net.http.websocket.MessageQueue}
 * -
 * Used by the {@link TransactionManager} to
 * create the input keys.
 * After the query is finished, the keys don't need to be returned to the cache,
 * but rather, the cache may need to grow to accommodate more queries.
 */
public final class KeyBuilder {

    public static final int DEFAULT_KEY_OBJECT_CACHE_SIZE = 10;

    private static final IntKey[] objects;

    private static int head;

    static {
        objects = new IntKey[DEFAULT_KEY_OBJECT_CACHE_SIZE];
        head = 0;
        for(int i = 0; i < DEFAULT_KEY_OBJECT_CACHE_SIZE; i++)
            objects[i] = IntKey.of();
    }

    public static IKey of(int hash){
        int currHead = head;
        head = (currHead + 1) & (DEFAULT_KEY_OBJECT_CACHE_SIZE - 1);
        objects[currHead].value = hash;
        return objects[currHead];
    }

}
