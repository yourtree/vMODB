package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.table.HashIndexedTable;
import org.junit.Test;

import java.util.List;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;

public class PlannerTest {

    private QueryTree getJoinQueryTree() throws BuilderException, AnalyzerException {

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes );
        catalog.insertTable( new HashIndexedTable( "tb1", schema, new int[]{0} ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema, new int[]{0} ));

        IQueryBuilder builder = QueryBuilderFactory.init();
        IStatement sql = builder.select("tb1.col1, tb2.col2") // this should not raise an error
                .from("tb1")
                .join("tb2","col1").on(EQUALS, "tb1.col1") // this may be ok but I am being strict here
                .where("tb1.col2", EQUALS, 2) // this should not raise an error
                .build();

        Analyzer analyzer = new Analyzer(catalog);
        QueryTree queryTree = analyzer.analyze( sql );


        return queryTree;
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

    private QueryTree getSimpleQueryTree() throws BuilderException, AnalyzerException {

        final Catalog catalog = new Catalog();
        String[] columnNames = { "col1", "col2" };
        DataType[] columnDataTypes = { DataType.INT, DataType.INT };
        final Schema schema = new Schema( columnNames, columnDataTypes );
        catalog.insertTable( new HashIndexedTable( "tb1", schema, new int[]{0} ));
        catalog.insertTable( new HashIndexedTable( "tb2", schema, new int[]{0} ));

        IQueryBuilder builder = QueryBuilderFactory.init();
        IStatement sql = builder.select("col1, col2")
                                .from("tb1")
                                .where("col1", EQUALS, 1)
                                .and("col2", EQUALS, 2)
                                .build();

        Analyzer analyzer = new Analyzer(catalog);
        QueryTree queryTree = analyzer.analyze( sql );
        return queryTree;
    }

    @Test
    public void testJoinPlan() throws BuilderException, AnalyzerException {

        final QueryTree queryTree = getJoinQueryTree();

        final Planner planner = new Planner();

        // planner.plan()

        assert true;
    }

    @Test
    public void testSimplePlan() throws BuilderException, AnalyzerException {
        final QueryTree queryTree = getSimpleQueryTree();
        assert true;
    }

    @Test
    public void testFilterCombinations(){

        Planner planner = new Planner();

        int[] filters = { 1, 2, 3, 4 };

        List<int[]> list = planner.getAllPossibleColumnCombinations( filters );

        assert  list != null;

    }


}
