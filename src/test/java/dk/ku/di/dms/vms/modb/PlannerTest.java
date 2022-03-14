package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilderException;
import org.junit.Test;

import java.util.List;

public class PlannerTest {

    @Test
    public void testJoinPlan() throws AnalyzerException, FilterBuilderException {

        final QueryTree queryTree = TestCommon.getJoinQueryTree();

        final Planner planner = new Planner();

        PlanNode node = planner.plan( queryTree );

        assert node != null;
    }

    @Test
    public void testSimplePlan() throws AnalyzerException {
        final QueryTree queryTree = TestCommon.getSimpleQueryTree();
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
