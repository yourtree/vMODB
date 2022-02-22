package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.row.CompositeKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.HashIndexedTable;
import dk.ku.di.dms.vms.database.store.table.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;

public class QueryTest {

    private static Catalog catalog;

    @BeforeClass
    public static void buildCatalog(){

        CompositeKey key = new CompositeKey(3,2L);

        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        DataType[] dataTypes = { DataType.INT, DataType.FLOAT, DataType.STRING, DataType.STRING  };

        Schema schema = new Schema(itemColumns, dataTypes);

        Table table = new HashIndexedTable("item", schema);

        catalog = new Catalog();
        catalog.insertTable(table);

        Row row = new Row(3,2L,"HAHA","HEHE");

        table.upsert( key, row);

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
        final Schema schema = new Schema( columnNames, columnDataTypes );
        catalog.insertTable( new HashIndexedTable( "tb1", schema, new int[]{0} ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema, new int[]{0} ));

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

        assert(true);

    }

}
