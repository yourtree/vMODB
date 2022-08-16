package dk.ku.di.dms.vms.modb.query.insert;

import java.util.function.Function;

/**
 * We can have many read-only transactions, those acquiring data for other microservices
 * but only a single write query. these can interleave arbitrarily
 * as long as the read queries get consistent snapshots of the data items
 *
 * I believe this is the fastest way to build serializable DB
 */
public class Insert implements Function<Object, Long> {

    /**
     * Returns the address
     * @param o What is the object?
     * @return address
     */
    @Override
    public Long apply(Object o) {
        return null;
    }
}
