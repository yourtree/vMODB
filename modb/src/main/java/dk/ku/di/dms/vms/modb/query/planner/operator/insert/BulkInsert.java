package dk.ku.di.dms.vms.modb.query.planner.operator.insert;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.schema.Schema;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.table.HashIndexedTable;
import dk.ku.di.dms.vms.modb.table.Table;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

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
public class BulkInsert implements Runnable, ContinuousStreamOperator, CloseableOperator {

    private volatile boolean signalEnd = false;

    // to scan tables holding the foreign keys
    private ExecutorService taskExecutor;

    private ByteBuffer tableBuffer;

    private Function<IKey, Integer> hashFunction;

    private Map<Table, Seek> tableToSeekOperationMap;

    private Table table;

    private BlockingQueue<ByteBuffer> inputQueue;

    private int bulkSize;

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

    public BulkInsert(ByteBuffer tableBuffer,
                      Function<IKey, Integer> hashFunction,
                      Table table,
                      Map<Table, Seek> tableToSeekOperationMap, // seek operations
                      BlockingQueue<ByteBuffer> inputQueue,
                      int bulkSize,
                      boolean atomic, // the bulk must be treated as atomic unit? i.e., one fails, all rows fail?
                      ExecutorService taskExecutor){
        this.tableBuffer = tableBuffer;
        this.hashFunction = hashFunction;
        this.table = table;
        this.inputQueue = inputQueue;
        this.tableToSeekOperationMap = tableToSeekOperationMap;
        this.bulkSize = bulkSize;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void run() {

        List<ByteBuffer> records = new ArrayList<>(bulkSize + 10);

        // with parallelism active, seek operations are handled by a thread pool
        if(taskExecutor != null) {

            Map<Table, int[]> fks = table.getForeignKeysGroupedByTable();

            // what about the foreign keys? let's do this parallelly
            CompletableFuture<?>[] futures = new CompletableFuture[fks.size()];

            // submit various and get result, but that would incur in several creation of objects, better to keep the seek active for various seek operations
            int j = 0;
            for (Table fkTable : fks.keySet()) {
                Seek seekOp = tableToSeekOperationMap.get(fkTable);
                futures[j] = CompletableFuture.runAsync(
                        seekOp, taskExecutor
                );
                j++;
            }

            // TODO finish

            // while seek tasks execute, do another task, look inner to this table...

        } else {

            do {

                int size = inputQueue.size();
                if(size > 0){
                    inputQueue.drainTo(records);

                    // don't necessarily need to wait till the end of the stream
                    process( records );
                    records.clear();

                } else {

                    try {
                        // block until 
                        ByteBuffer buffer = inputQueue.take();
                        records.add(buffer);
                    } catch (InterruptedException ignored) {}

                }

            } while(!signalEnd && inputQueue.size() > 0);

            //
            process( records );

        }

        close();

    }
    
    private void process(List<ByteBuffer> records){

        Map<Table, int[]> fks = table.getForeignKeysGroupedByTable();

        for(ByteBuffer record : records){

            // the table must contain the reference of the buffer, so we can query it

            Schema schema = table.getSchema();

            boolean violation = false;

            // for each make input and collect later
            for(Table fkTable : fks.keySet()){

                // how would a seek execute?

                // 1 - find the record, maybe through an index?

                // 2 - does the record exists in the table?

                // 3 - if not, violated = true

            }

            // considering the buffer position starts at 0
            Map<Integer, ConstraintReference> constraints = schema.constraints();

            int[] columnOffset = schema.columnOffset();

            for(Map.Entry<Integer, ConstraintReference> constraintEntry : constraints.entrySet()) {

                var constraintType = constraintEntry.getValue().constraint;
                if (constraintType == ConstraintEnum.NOT_NULL) {

                    // can also do - next, but better to get that from the enum
                    // just need to know how many bytes are in there
                    record.position(columnOffset[constraintEntry.getKey()]);

                    switch (schema.getColumnDataType(constraintEntry.getValue().column)) {

                        // primitive number and byte types are always initialized as 0, cannot tell whether it is null or not... leave it as 0, when updating then can check better

                        // char and date can be checked
                        case CHAR, DATE -> {
                            // is first byte not null? it is sufficient to check in this way?
                            violation = tableBuffer.get() != ZERO;
                        }
                        // default do nothing

                    }

                } else {

                    switch (schema.getColumnDataType(constraintEntry.getKey())) {

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
                    break;
                }

            }

            if (violation) {
                continue; // may cancel this entire operation if atomic is on
            }

            // everything fine for this row
            tableBuffer.put(record);

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

    // send signal end of stream to all tasks and close everything else
    private void close() {

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
