package dk.ku.di.dms.vms.modb.query;

import dk.ku.di.dms.vms.modb.ExecutorTest;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.EQUALS;

/**
 * Common set of methods for two or more tests (e.g., {@link PlannerTest} and {@link ExecutorTest}
 */
public final class TestCommon {

    public static Catalog getDefaultCatalog(){

        // item
        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        DataType[] itemDataTypes = { DataType.INT, DataType.FLOAT, DataType.STRING, DataType.STRING };
        Schema itemSchema = new Schema(itemColumns, itemDataTypes, new int[]{0}, null );

        ResourceScope scope = ResourceScope.newSharedScope();
        MemorySegment segment = MemorySegment.allocateNative(itemSchema.getRecordSize() * 10L, scope);
        RecordBufferContext rbc = new RecordBufferContext(segment, 10, itemSchema.getRecordSize());
        UniqueHashIndex index = new UniqueHashIndex(rbc, itemSchema, itemSchema.getPrimaryKeyColumns());
        Table itemTable = new Table("item", itemSchema, index);

        // customer
//        String[] customerColumns = { "c_id", "c_d_id", "c_w_id", "c_discount", "c_last", "c_credit", "c_balance", "c_ytd_payment" };
//        DataType[] customerDataTypes = { DataType.LONG, DataType.INT, DataType.INT,
//                DataType.FLOAT, DataType.STRING, DataType.STRING, DataType.FLOAT, DataType.FLOAT };
//        Schema customerSchema = new Schema(customerColumns, customerDataTypes, new int[]{0,1,2}, null );
//        Table customerTable = new Table("customer", customerSchema);
//
        Catalog catalog = new Catalog();
        catalog.insertTables(itemTable);

        return catalog;

    }

    public static QueryTree getSimpleQueryTree() throws AnalyzerException {

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes, new int[]{0}, null );
        catalog.insertTable( new Table( "tb1", schema,  (UniqueHashIndex) null ));
        catalog.insertTable( new Table( "tb2", schema,  (UniqueHashIndex) null ));

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        IStatement sql = builder.select("col1, col2")
                .from("tb1")
                .where("col1", EQUALS, 1)
                .and("col2", EQUALS, 2)
                .build();

        Analyzer analyzer = new Analyzer(catalog);
        QueryTree queryTree = analyzer.analyze( sql );
        return queryTree;
    }

    public static QueryTree getJoinQueryTree() throws AnalyzerException {

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes, new int[]{0}, null );
        catalog.insertTable( new Table( "tb1", schema, (UniqueHashIndex) null ));
        catalog.insertTable( new Table( "tb2", schema,  (UniqueHashIndex) null ));
        catalog.insertTable( new Table( "tb3", schema,  (UniqueHashIndex) null ));

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        IStatement sql = builder.select("tb1.col1, tb2.col2")
                .from("tb1")
                .join("tb2","col1").on(EQUALS, "tb1", "col1")
                .join("tb3","col1").on(EQUALS, "tb1", "col1")
                .where("tb1.col2", EQUALS, 2)
                .and("tb2.col2",EQUALS,1)
                .build();

        Analyzer analyzer = new Analyzer(catalog);
        QueryTree queryTree = analyzer.analyze( sql );

        return queryTree;
    }

}
