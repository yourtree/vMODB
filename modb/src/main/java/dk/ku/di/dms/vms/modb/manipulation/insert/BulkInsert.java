package dk.ku.di.dms.vms.modb.manipulation.insert;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.definition.Table;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/**
 * I can keep all primary indexes in mapped byte buffer
 * Hash table representation
 * The other (secondary) indexes can be built in memory maybe?
 *
 * Receive byte buffers for insertion
 *
 * Only supporting hash index now. Make sure all constraints are respected
 *
 * The input receives parsed rows. It means that another operator (o system component) must transform the entity into a buffer abstraction
 *
 */
public class BulkInsert implements Runnable,
        ContinuousInputStreamOperator, BoundedInputStreamOperator {

    private static final int DEFAULT_BULK_SIZE = 100;

    private volatile boolean signalEnd = false;

    private final Table table;

    private final BlockingQueue<ByteBuffer> inputQueue;

    private final boolean atomic;

    /*
     * Arrays for local processing
     */
    List<ByteBuffer> records = new ArrayList<>(DEFAULT_BULK_SIZE + 10);

    /**
     * What do we need?
     *
     * Instead of consumer and producer APIs, we can use
     * blocking queues. These queues can be reusable across
     * query plans and serve to store what records must be
     * processed by the upstream operator
     *
     * The insert operator does not need an output queue
     * But requires an input queue.
     */

    public BulkInsert(Table table,
                      // Map<Table, SeekTask> tableToSeekOperationMap, // seek operations
                      BlockingQueue<ByteBuffer> inputQueue,
                      // int bulkSize,
                      boolean atomic // the bulk must be treated as atomic unit? i.e., one fails, all rows fail?
                      // ExecutorService taskExecutor
    ){
        this.table = table;
        this.inputQueue = inputQueue;
        this.atomic = atomic;
    }

    @Override
    public void run() {
//
//         with parallelism active, seek operations are handled by a thread pool
//        if(taskExecutor != null) {
//
//            Map<Table, int[]> fks = table.getForeignKeysGroupedByTable();
//
//            // what about the foreign keys? let's do this parallelly
//            CompletableFuture<?>[] futures = new CompletableFuture[fks.size()];
//
//            // submit various and get result, but that would incur in several creation of objects, better to keep the seek active for various seek operations
//            int j = 0;
//            for (Table fkTable : fks.keySet()) {
//                SeekTask seekOp = tableToSeekOperationMap.get(fkTable);
////                futures[j] = CompletableFuture.runAsync(
////                        seekOp, taskExecutor
////                );
//                j++;
//            }

            // while seek tasks execute, do another task, look inner to this table...

//        } else {

        // if atomic, better wait for all records to arrive
        if(atomic){
            while(!signalEnd){
                int size = inputQueue.size();
                if(size > 0){
                    inputQueue.drainTo(records);
                } else {
                    try {
                        // block until
                        records.add(inputQueue.take());
                    } catch (InterruptedException ignored) {}

                }
            }
            processAtomically();
            return;
        }

        do {

            int size = inputQueue.size();
            if(size > 0){
                inputQueue.drainTo(records);
                // don't necessarily need to wait till the end of the stream
                process( );
                records.clear();
            } else {

                try {
                    // block until
                    records.add(inputQueue.take());
                } catch (InterruptedException ignored) {}

            }

        } while(!signalEnd && inputQueue.size() > 0);

        // process remaining, if so
        process( );

        // }

    }

    private boolean safeguardFKs(ByteBuffer record){

        for(Map.Entry<Table,int[]> fkTable : table.getForeignKeysGroupedByTable().entrySet()){

            // how would a seek execute?

            // a fk is always a PK of the other table
//            IKey pk = KeyUtils.buildRecordKey( fkTable.getKey().getSchema(), record, fkTable.getValue() );
//
//            // 2 - does the record exists in the table?
//            if( ! fkTable.getKey().getPrimaryKeyIndex().exists( pk ) )
//                return false;

        }

        return true;

    }

    private boolean safeguardOtherConstraints(ByteBuffer record) {

        boolean violation = false;

        // considering the buffer position starts at 0
        Map<Integer, ConstraintReference> constraints = table.getSchema().constraints();

        int[] columnOffset = table.getSchema().columnOffset();

        for(Map.Entry<Integer, ConstraintReference> constraintEntry : constraints.entrySet()) {

            var constraintType = constraintEntry.getValue().constraint;
            if (constraintType == ConstraintEnum.NOT_NULL) {

                // can also do - next, but better to get that from the enum
                // just need to know how many bytes are in there
                record.position(columnOffset[constraintEntry.getKey()]);

                switch (table.getSchema().getColumnDataType(constraintEntry.getValue().column)) {

                    // primitive number and byte types are always initialized as 0, cannot tell whether it is null or not... leave it as 0, when updating then can check better

                    // char and date can be checked
                    case CHAR, DATE -> // is first byte not null? it is sufficient to check in this way?
                            violation = false;// TODO adapt record.get() != inactive;

                    // default do nothing

                }

            } else {

                switch (table.getSchema().getColumnDataType(constraintEntry.getKey())) {

                    case INT -> {
                        // how is 0 stored in bytes? 0 0 0 0 ? if so, cannot define whether it is null or not
                        int read = record.getInt(); // this is misleading, need to check the actual bytes, but could also be 0
                        violation = ConstraintHelper.eval(read, 0, Integer::compareTo, constraintType);
                    }
                    case LONG -> {
                        long read = record.getLong();
                        violation = ConstraintHelper.eval(read, 0L, Long::compareTo, constraintType);
                    }
                    case FLOAT -> {
                        float read = record.getFloat();
                        violation = ConstraintHelper.eval(read, 0f, Float::compareTo, constraintType);
                    }
                    case DOUBLE -> {
                        double read = record.getDouble();
                        violation = ConstraintHelper.eval(read, 0d, Double::compareTo, constraintType);
                    }

                }
            }

            if (violation) {
                break; // don't need to check other constraints
            }

        }

        return violation;

    }

    private void processAtomically(){

        boolean violation = false;

        for(ByteBuffer record : records){

            // the table must contain the reference of the buffer, so we can query it
            violation = safeguardFKs(record);

            if(violation) break;// go to next record

            violation = safeguardOtherConstraints(record);

            if (violation) break;

        }

        if(violation) return;

        for(ByteBuffer record : records){
//            IKey recordKey = KeyUtils.buildRecordKey( table.getSchema(), record, table.getSchema().getPrimaryKeyColumns() );
//            table.getPrimaryKeyIndex().insert( recordKey, MemoryUtils.getByteBufferAddress(record) );
        }

    }

    private void process(){

        boolean violation;

        for(ByteBuffer record : records){

            // the table must contain the reference of the buffer, so we can query it
            violation = safeguardFKs(record);

            if(violation) continue;// go to next record

            violation = safeguardOtherConstraints(record);

            if (violation) continue;

            // build key
            // 1 - get columns that form the PK from the schema
//            IKey recordKey = KeyUtils.buildRecordKey( table.getSchema(), record, table.getSchema().getPrimaryKeyColumns() );
//
//            // everything fine for this row
//            table.getPrimaryKeyIndex().insert( recordKey, MemoryUtils.getByteBufferAddress(record) );

        }
        
    }

    // comparator helper... caches and provides comparators for usage across queries
    // move outside this class to avoid multiple instantiations
    private static class ConstraintHelper {

        public static <T> boolean eval(T v1, T v2, Comparator<T> comparator, ConstraintEnum constraint){

            if(constraint == ConstraintEnum.POSITIVE_OR_ZERO){
                return comparator.compare(v1, v2) >= 0;
            }
            if(constraint == ConstraintEnum.POSITIVE){
                return comparator.compare(v1, v2) > 0;
            }
            return false;
        }

    }

    @Override
    public void accept(ByteBuffer buffer) {
        this.inputQueue.add( buffer );
    }

    @Override
    public void signalEndOfStream() {
        this.signalEnd = true;
    }

}
