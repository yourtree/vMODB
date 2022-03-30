package dk.ku.di.dms.vms;


import dk.ku.di.dms.vms.modb.catalog.Catalog;
import dk.ku.di.dms.vms.modb.common.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.common.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.store.meta.Schema;
import dk.ku.di.dms.vms.modb.store.table.HashIndexedTable;
import dk.ku.di.dms.vms.modb.store.table.Table;

import static dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum.EQUALS;

/**
 * Common set of methods for two or more tests (e.g., {@link PlannerTest} and {@link ExecutorTest}
 */
public final class TestCommon {

    public static Catalog getDefaultCatalog(){

        // item
        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        DataType[] itemDataTypes = { DataType.INT, DataType.FLOAT, DataType.STRING, DataType.STRING  };
        Schema itemSchema = new Schema(itemColumns, itemDataTypes, new int[]{0}, null );
        Table itemTable = new HashIndexedTable("item", itemSchema);

        // customer
        String[] customerColumns = { "c_id", "c_d_id", "c_w_id", "c_discount", "c_last", "c_credit", "c_balance", "c_ytd_payment" };
        DataType[] customerDataTypes = { DataType.LONG, DataType.INT, DataType.INT,
                DataType.FLOAT, DataType.STRING, DataType.STRING, DataType.FLOAT, DataType.FLOAT };
        Schema customerSchema = new Schema(customerColumns, customerDataTypes, new int[]{0,1,2}, null );
        Table customerTable = new HashIndexedTable("customer", customerSchema);

        Catalog catalog = new Catalog();

        catalog.insertTables(itemTable,customerTable);

        return catalog;

    }

    public static QueryTree getSimpleQueryTree() throws AnalyzerException {

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes, new int[]{0}, null );
        catalog.insertTable( new HashIndexedTable( "tb1", schema ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema ));

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
        catalog.insertTable( new HashIndexedTable( "tb1", schema ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema ));
        catalog.insertTable( new HashIndexedTable( "tb3", schema ));

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
