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
 */
public class BulkInsert implements Runnable, ContinuousStreamOperator, CloseableOperator {

    private volatile boolean signalEnd = false;

    // to scan tables holding the foreign keys
    private ExecutorService taskExecutor;

    private ByteBuffer buffer;

    private Function<IKey, Integer> hashFunction;

    private Map<Table, Seek> tableToSeekOperationMap;

    private Table table;

    private BlockingQueue<ByteBuffer> inputQueue;

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

    public BulkInsert(ByteBuffer buffer,
                      Function<IKey, Integer> hashFunction,
                      Table table,
                      Map<Table, Seek> tableToSeekOperationMap, // seek operation
                      BlockingQueue<ByteBuffer> inputQueue,
                      ExecutorService taskExecutor){
        this.buffer = buffer;
        this.hashFunction = hashFunction;
        this.table = table;
        this.inputQueue = inputQueue;
        this.tableToSeekOperationMap = tableToSeekOperationMap;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void run() {

        // with parallelism active, seek operations are handled by thread pool


        // init parallel
        Map<Table, int[]> fks = table.getForeignKeysGroupedByTable();

        CompletableFuture<?>[] futures = new CompletableFuture[fks.size()];

        // submit various and get result, but that would incur in several creation of objects, better to keep the seek active for various seek operations
        int j = 0;
        for(Table fkTable : fks.keySet()){
            Seek seekOp = tableToSeekOperationMap.get( fkTable );
            futures[j] = CompletableFuture.runAsync(
                    seekOp, taskExecutor
            );
            j++;
        }

        while(!signalEnd){

            int size = inputQueue.size();
            if(size > 0){
                List<ByteBuffer> records = new ArrayList<>(size+10);
                inputQueue.drainTo(records);
            } else {

                try {


                    // these are views over a direct memory buffer
                    ByteBuffer appBuffer = inputQueue.take();

                    // what about the foreign keys? let's do this parallelly
                    // the table must contain the reference of the buffer, so we can query it

                    Schema schema = table.getSchema();

                    // for each make input and collect later
                    for(Table fkTable : fks.keySet()){

                    }


                    // while seek tasks execute, do another task, look inner to this table...





                    // considering the buffer position starts at 0
                    Map<Integer, ConstraintReference> constraints = schema.constraints();

                    int[] columnOffset = schema.columnOffset();

                    boolean violation = false;

                    int idx = 0;
                    for(Map.Entry<Integer, ConstraintReference> constraintEntry : constraints.entrySet()){



                        var constraintType = constraintEntry.getValue().constraint;
                        if(constraintType == ConstraintEnum.NOT_NULL || constraintType == ConstraintEnum.NULL){

                            // check whether all bytes are zero

                            // can also do - next, but better to get that from the enum
                            // just need to know how many bytes are in there
                            buffer.position(columnOffset[constraintEntry.getKey()]);

                            switch (schema.getColumnDataType(constraintEntry.getValue().column)) {

                                // primitive number and byte types are always initialized as 0, cannot tell whether it is null or not... leave it as 0, when updating then can check better


                                // char and date can be checked

                                case CHAR -> {

                                }

                                case DATE -> {

                                }

                                // default do nothing

                            }

                        } else {

                            switch (schema.getColumnDataType(idx)) {

                                case INT -> {
                                    // how is 0 stored in bytes? 0 0 0 0 ? if so, cannot define whether it is null or not
                                    int read = buffer.getInt(); // this is misleading, need to check the actual bytes, but could also be 0

                                    violation = ConstraintHelper.eval( read, Integer::compareTo, constraintType );

                                }

                            }
                        }

                        if(violation){
                            break;
                        }
                        // comparator helper... caches and provides comparators for usage across queries
                        idx++;
                    }


                    /// collect now
                    if(violation){
                        // no need to collect result
                        break;
                    }

                } catch (InterruptedException ignored) {}


            }



        }

        int size = inputQueue.size();
        if(size > 0){
            List<ByteBuffer> records = new ArrayList<>(size+10);
            inputQueue.drainTo(records);

            // process

        }

        close();

    }

    // move outise this class to avoid multiple instantiations
    private static class ConstraintHelper {

        public static <T> boolean eval(T v, Comparator<T> comparator, ConstraintEnum constraint){

            if(constraint == ConstraintEnum.NULL){
                if (v == null) return true;
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
