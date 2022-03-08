package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.query.planner.node.projection.Projector;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
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

public class QueryTest {

    private static Catalog catalog;

    @BeforeClass
    public static void buildCatalog(){

        catalog = TestCommon.getDefaultCatalog();

        Table table = catalog.getTable("item");

        CompositeKey key = new CompositeKey(3,2L);
        Row row = new Row(3,2L,"HAHA","HEHE");

        table.getPrimaryKeyIndex().upsert(key, row);

    }

    @Test
    public void isCatalogNotEmpty() throws Exception {
        assert catalog.getTable("item") != null;
    }

    @Test
    public void testQueryParse(){

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes, new int[]{0} );
        catalog.insertTable( new HashIndexedTable( "tb1", schema ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema ));

        try {
            // TODO move this test to query test
            IQueryBuilder builder = QueryBuilderFactory.init();
            IStatement sql = builder.select("col1, col2") // this should raise an error
                    .from("tb1")
                    .join("tb2","col1").on(EQUALS, "tb2.col1")
                    .where("col2", EQUALS, 2) // this should raise an error
                    .build();

            Analyzer analyzer = new Analyzer(catalog);
            QueryTree queryTree = analyzer.analyze( sql );

        } catch(AnalyzerException | BuilderException e){
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

        Projector projector = new Projector(CustomerInfoDTO.class, columnReferenceList);

        Collection<Row> rows = new ArrayList<>(2);
        Collections.addAll(rows, new Row(1F,"1","1" ) );

        OperatorResult operatorResult = new OperatorResult( rows );

        projector.accept( operatorResult );

        Object object = projector.get();

        assert(object != null);

    }

    @Test
    public void testQueryParsing() throws Exception {

        IQueryBuilder builder = QueryBuilderFactory.init();
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
