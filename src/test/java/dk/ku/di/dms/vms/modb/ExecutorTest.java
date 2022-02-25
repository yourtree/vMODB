package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilderException;
import org.junit.Test;

public class ExecutorTest {

    public void ingestSyntheticData(){

        // get tables and their reference,
        // ingest the data according to a join strategy: several matches, low matches, no matches

    }

    @Test
    public void testExecutor() throws BuilderException, AnalyzerException, FilterBuilderException {

//        final QueryTree queryTree = getJoinQueryTree();
//
//        final Planner planner = new Planner();
//
//        PlanNode node = planner.plan( queryTree );
//
//        assert node != null;
    }


}
