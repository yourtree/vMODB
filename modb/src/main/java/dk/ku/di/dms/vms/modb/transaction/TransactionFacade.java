package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A transaction management facade
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 * Repository facade parses the request. Transaction facade deals with low-level operations
 * Batch-commit aware. That means when a batch comes, must make data durable.
 * in order to accommodate two or more VMSs in the same resource,
 *  it would need to make this class an instance (no static methods) and put it into modb modules
 */
public final class TransactionFacade {

    private static final ThreadLocal<Set<PrimaryIndex>> INDEX_WRITES = ThreadLocal.withInitial( () -> {
        if(!TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
            return new HashSet<>(2);
        }
        return Collections.emptySet();
    });

    /**
     * Hashed by table name
     */
    private final Map<String, Table> tableMap;

    private final Analyzer analyzer;

    private final SimplePlanner planner;

    /**
     * Operators output results
     * They are read-only operations, do not modify data
     */
    private final Map<String, AbstractSimpleOperator> readQueryPlans;

    private TransactionFacade(Map<String, Table> tableMap){
        this.tableMap = tableMap;
        this.planner = new SimplePlanner();
        this.analyzer = new Analyzer(tableMap);
        // read-only transactions may put items here
        this.readQueryPlans = new ConcurrentHashMap<>();
    }

    public static TransactionFacade build(Map<String, Table> catalog){
        return new TransactionFacade(catalog);
    }

    /**
     * Why references to foreign key constraints are here?
     * Because an index should not know about other indexes.
     * It is better to have an upper class taking care of this constraint.
     * Can be made parallel.
     */
    private boolean fkConstraintViolation(Table table, Object[] values){
        for(var entry : table.foreignKeys().entrySet()){
            IKey fk = KeyUtils.buildRecordKey( entry.getValue(), values );
            // have some previous TID deleted it? or simply not exists
            if (!entry.getKey().exists(fk)) return true;
        }
        return false;
    }

    /**
     * Best guess return type. Differently from the parameter type received.
     * @param selectStatement a select statement
     * @return the query result in a memory space
     */
    public MemoryRefNode fetch(PrimaryIndex consistentIndex, SelectStatement selectStatement) {

        String sqlAsKey = selectStatement.SQL.toString();

        AbstractSimpleOperator scanOperator = this.readQueryPlans.get( sqlAsKey );

        List<WherePredicate> wherePredicates;

        if(scanOperator == null){
            QueryTree queryTree;
            try {
                queryTree = this.analyzer.analyze(selectStatement);
                wherePredicates = queryTree.wherePredicates;
                scanOperator = this.planner.plan(queryTree);
                readQueryPlans.put(sqlAsKey, scanOperator );
            } catch (AnalyzerException ignored) { return null; }

        } else {
            // get only the where clause params
            try {
                wherePredicates = this.analyzer.analyzeWhere(
                        scanOperator.asScan().table, selectStatement.whereClause);
            } catch (AnalyzerException ignored) { return null; }
        }

        MemoryRefNode memRes = null;

        // TODO complete for all types or migrate the choice to transaction facade
        //  make an enum, it is easier
        if(scanOperator.isIndexScan()){
            // build keys and filters
            //memRes = OperatorExecution.run( wherePredicates, scanOperator.asIndexScan() );
            memRes = this.run(consistentIndex, wherePredicates, scanOperator.asIndexScan());
        } else {
            // build only filters
            memRes = this.run( consistentIndex, wherePredicates, scanOperator.asFullScan() );
        }

        return memRes;

    }

    /**
     * TODO finish. can we extract the column values and make a special api for the facade? only if it is a single key
     */
    public void issue(Table table, IStatement statement) throws AnalyzerException {

        switch (statement.getType()){
            case UPDATE -> {

                List<WherePredicate> wherePredicates = this.analyzer.analyzeWhere(
                        table, statement.asUpdateStatement().whereClause);

                this.planner.getOptimalIndex(table, wherePredicates);

                // TODO plan update and delete in planner. only need to send where predicates and not a query tree like a select
                // UpdateOperator.run(statement.asUpdateStatement(), table.primaryKeyIndex() );
            }
            case INSERT -> {

                // TODO get columns, put object array in order and submit to entity api

            }
            case DELETE -> {

            }
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }

    }

    /**
     * It installs the writes without taking into consideration concurrency control.
     * Used when the buffer received is aligned with how the data is stored in memory
     */
    public void bulkInsert(Table table, ByteBuffer buffer, int numberOfRecords){
        // if the memory address is occupied, must log warning
        // so we can increase the table size
        long address = MemoryUtils.getByteBufferAddress(buffer);

        // if the memory address is occupied, must log warning
        // so we can increase the table size
        ReadWriteIndex<IKey> index = table.underlyingPrimaryKeyIndex();
        int sizeWithoutHeader = table.schema.getRecordSizeWithoutHeader();
        long currAddress = address;

        IKey key;
        for (int i = 0; i < numberOfRecords; i++) {
            key = KeyUtils.buildPrimaryKey(table.schema, currAddress);
            index.insert( key, currAddress );
            currAddress += sizeWithoutHeader;
        }

    }

    /****** ENTITY *******/

    public void insertAll(Table table, List<Object[]> objects){
        // get tid, do all the checks, etc
        for(Object[] entry : objects) {
            this.insert(table, entry);
        }
    }

    public void deleteAll(Table table, List<Object[]> objects) {
        for(Object[] entry : objects) {
            this.delete(table.primaryKeyIndex(), entry);
        }
    }

    public void updateAll(Table table, List<Object[]> objects) {
        for(Object[] entry : objects) {
            this.update(table, entry);
        }
    }

    /**
     * Not yet considering this record can serve as FK to a record in another table.
     */
    public void delete(PrimaryIndex index, Object[] values) {
        IKey pk = KeyUtils.buildPrimaryKey(index.schema(), values);
        this.deleteByKey(index, pk);
    }

    public void deleteByKey(PrimaryIndex index, Object[] values) {
        IKey pk = KeyUtils.buildInputKey(values);
        this.deleteByKey(index, pk);
    }

    /**
     * TODO not considering foreign key. must prune the other tables inside this VMS
     * @param index The corresponding database index
     * @param pk The primary key
     */
    private void deleteByKey(PrimaryIndex index, IKey pk){
        if(index.delete(pk)){
            INDEX_WRITES.get().add(index);
        }
    }

    public Object[] lookupByKey(PrimaryIndex index, Object... valuesOfKey){
        IKey pk = KeyUtils.buildPrimaryKeyFromKeyValues(valuesOfKey);
        return index.lookupByKey(pk);
    }

    /**
     * @param table The corresponding database table
     * @param values The fields extracted from the entity
     */
    public void insert(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(!fkConstraintViolation(table, values) && index.insert(pk, values)){
            INDEX_WRITES.get().add(index);

            // iterate over secondary indexes to insert the new write
            // this is the delta. records that the underlying index does not know yet
            for(NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()){
                secIndex.write( pk, values );
            }

            return;
        }
        undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    public Object insertAndGet(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        // IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(!fkConstraintViolation(table, values)){
            IKey key_ = index.insertAndGet(values);
            if(key_ != null) {
                INDEX_WRITES.get().add(index);
                for(NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()){
                    secIndex.write( key_, values );
                }
                return values[ table.primaryKeyIndex().columns()[0] ];
            }
        }
        undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    /**
     * Iterate over all indexes, get the corresponding writes of this tid and remove them
     *      this method can be called in parallel by transaction facade without risk
     */
    public void update(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(!fkConstraintViolation(table, values) && index.update(pk, values)){
            INDEX_WRITES.get().add(index);
            return;
        }
        undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    private void undoTransactionWrites(){
        for(var index : INDEX_WRITES.get()) {
            index.undoTransactionWrites();
        }
    }

    /****** SCAN OPERATORS *******/

    public MemoryRefNode run(PrimaryIndex consistentIndex,
                             List<WherePredicate> wherePredicates,
                             IndexScanWithProjection operator){

        List<Object> keyList = new ArrayList<>(operator.index.columns().length);
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.containsColumn( wherePredicate.columnReference.columnPosition )){
                keyList.add( wherePredicate.value );
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

        FilterContext filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);

        // build input
        IKey inputKey = KeyUtils.buildInputKey(keyList.toArray());

        return operator.run( consistentIndex, filterContext, inputKey );
    }

    public MemoryRefNode run(PrimaryIndex consistentIndex,
                             List<WherePredicate> wherePredicates,
                             FullScanWithProjection operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return operator.run( consistentIndex, filterContext );
    }

    /****** WRITE OPERATORS *******/



    /* CHECKPOINTING AND LOGGING *******/

    /**
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     */
    public void checkpoint(){

        // make state durable
        // get buffered writes in transaction facade and merge in memory
        var indexes = INDEX_WRITES.get();

        for(var index : indexes){
            index.installWrites();
            // log index since all updates are made
            // index.asUniqueHashIndex().buffer().log();
        }

        // TODO must modify corresponding secondary indexes too

    }

    public void log(){
        // TODO must log the updates in a separate file. no need for WAL, no need to store before and after
    }


}
