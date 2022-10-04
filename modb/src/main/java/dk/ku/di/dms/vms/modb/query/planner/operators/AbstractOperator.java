package dk.ku.di.dms.vms.modb.query.planner.operators;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.query.planner.operators.join.HashJoinWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;

public abstract class AbstractOperator {

    // the first node of the memory segment nodes
    protected MemoryRefNode memoryRefNode = null;

    protected AppendOnlyBuffer currentBuffer;

    protected final int entrySize;

    public AbstractOperator(int entrySize) {
        this.entrySize = entrySize;
    }

    /**
     * Just abstracts on which memory segment a result will be written to
     *
     * Default method. Operators can create their own
     */
    protected void ensureMemoryCapacity(){

        if(currentBuffer.capacity() - currentBuffer.address() > entrySize){
            return;
        }

        // else, get a new memory segment
        MemoryRefNode claimed = MemoryManager.getTemporaryDirectMemory();

        claimed.next = memoryRefNode;
        memoryRefNode = claimed;

        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());

    }

    /**
     * For operators that don't know the amount of records from start
     * @param size
     */
    protected void ensureMemoryCapacity(int size){

        if(currentBuffer.capacity() - currentBuffer.address() > size){
            return;
        }

        // else, get a new memory segment
        MemoryRefNode claimed = MemoryManager.getTemporaryDirectMemory(size);

        claimed.next = memoryRefNode;
        memoryRefNode = claimed;

        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());

    }

    // must be overridden by the concrete operators
    public boolean isFullScan(){
        return false;
    }

    public boolean isIndexScan(){
        return false;
    }

    public boolean isHashJoin() { return false; }

    public IndexScanWithProjection asIndexScan(){
        throw new IllegalStateException("No index scan operator");
    }

    public FullScanWithProjection asFullScan(){
        throw new IllegalStateException("No full scan operator");
    }

    public AbstractScan asScan(){
        throw new IllegalStateException("No abstract scan operator");
    }

    public HashJoinWithProjection asHashJoin() { throw new IllegalStateException("No hash join operator"); }

}
