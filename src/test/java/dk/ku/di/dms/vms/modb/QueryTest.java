package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.PlanTree;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.store.Table;
import dk.ku.di.dms.vms.tpcc.entity.District;
import org.junit.Test;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum.EQUALS;

public class QueryTest {

    private Catalog catalog;

    private void buildCatalog(){
        Table<District.DistrictId, District> tbl = new Table<>("district");

        this.catalog = new Catalog();
        catalog.tableMap.put(tbl.name,tbl);

        District district = new District();
        district.d_id = 3;
        district.d_w_id = 2;

        District.DistrictId id = new District.DistrictId(3,2);

        tbl.rows.put(id,district);
    }

    @Test
    public void testQueryParsing() throws Exception {

        buildCatalog();

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
        PlanTree planTree = planner.plan(queryTree);

        assert(true);

    }

}
