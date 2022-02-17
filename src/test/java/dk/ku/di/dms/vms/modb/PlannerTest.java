package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.database.query.planner.Planner;
import org.junit.Test;

import java.util.List;

public class PlannerTest {


    @Test
    public void testFilterCombinations(){

        Planner planner = new Planner();

        int[] filters = { 1, 2, 3, 4 };

        List<int[]> list = planner.getAllPossibleColumnCombinations( filters );

        assert  list != null;

    }


}
