package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.executor.SequentialQueryExecutor;
import dk.ku.di.dms.vms.database.query.parser.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.database.query.parser.builder.UpdateStatementBuilder;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.parser.stmt.SelectStatement;
import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.tree.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.query.planner.operator.projection.TypedProjector;
import dk.ku.di.dms.vms.database.store.common.SimpleKey;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.HashIndexedTable;
import dk.ku.di.dms.vms.database.store.table.Table;
import dk.ku.di.dms.vms.tpcc.dto.CustomerInfoDTO;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;
import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.GREATER_THAN;

public class QueryTest {

    private static Catalog catalog;

    @BeforeClass
    public static void buildCatalog(){

        catalog = TestCommon.getDefaultCatalog();

        Table table = catalog.getTable("item");

        SimpleKey key = new SimpleKey(3);
        Row row = new Row(3,2L,"HAHA","HEHE");

        SimpleKey key1 = new SimpleKey(4);
        Row row1 = new Row(4,3L,"HAHAdedede","HEHEdeded");

        table.getPrimaryKeyIndex().upsert(key, row);
        table.getPrimaryKeyIndex().upsert(key1, row1);
    }

    @Test
    public void isCatalogNotEmpty() {
        assert catalog.getTable("item") != null;
    }

    @Test
    public void testQueryParse(){

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes, new int[]{0}, null, null );
        catalog.insertTable( new HashIndexedTable( "tb1", schema ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema ));

        try {
            // TODO move this test to query test
            SelectStatementBuilder builder = QueryBuilderFactory.select();
            IStatement sql = builder.select("col1, col2") // this should raise an error
                    .from("tb1")
                    .join("tb2","col1").on(EQUALS, "tb1", "col1")//.and
                    .where("col2", EQUALS, 2) // this should raise an error
                    .build();

            Analyzer analyzer = new Analyzer(catalog);
            QueryTree queryTree = analyzer.analyze( sql );

        } catch(AnalyzerException e){
            System.out.println(e.getMessage());
            assert true;
        }

    }

    @Test
    public void testProjection(){

        ColumnReference column1 = new ColumnReference("c_discount",0);
        ColumnReference column2 = new ColumnReference("c_last",1);
        ColumnReference column3 = new ColumnReference("c_credit",2);

        List<ColumnReference> columnReferenceList = new ArrayList<>(3);
        columnReferenceList.add(column1);
        columnReferenceList.add(column2);
        columnReferenceList.add(column3);

        TypedProjector projector = new TypedProjector(CustomerInfoDTO.class, columnReferenceList);

        Collection<Row> rows = new ArrayList<>(2);
        Collections.addAll(rows, new Row(1F,"1","1" ) );

        OperatorResult operatorResult = new OperatorResult( rows );

        projector.accept( operatorResult );

        Object object = projector.get();

        assert(object != null);

    }

    @Test
    public void testGroupByQuery() throws Exception {

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        SelectStatement sql = builder //.select("AVG(item.i_price)")
                .avg("item.i_price").having(GREATER_THAN, 10)
                .from("item")
                //.groupBy("i_price")
                //.where("col2", EQUALS, 2) // this should raise an error
                .build();

        Analyzer analyzer = new Analyzer( catalog );
        QueryTree queryTree = analyzer.analyze( sql );

        Planner planner = new Planner();
        PlanNode planTree = planner.plan(queryTree);

        SequentialQueryExecutor queryExecutor = new SequentialQueryExecutor(planTree);

        OperatorResult result = queryExecutor.get();

        assert(result != null);

    }

    @Test
    public void testQueryParsing() throws Exception {

        UpdateStatementBuilder builder = QueryBuilderFactory.update();
        IStatement sql = builder.update("district")
                                .set("d_next_o_id",1)
                                .where("d_w_id", EQUALS, 2)
                                .and("d_id", EQUALS, 3)
                                .build();

        // TODO assert the set, where, and...

        // insert data, build metadata, query processing

        Analyzer analyzer = new Analyzer(catalog);

        QueryTree queryTree = analyzer.analyze(sql);

        Planner planner = new Planner();
        PlanNode planTree = planner.plan(queryTree);

        assert(planTree != null);

    }

}
