package dk.ku.di.dms.vms.modb.query.planner.execution;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Encapsulates the mutating aspects of a plan (input and filters)
 * as well as providing a set of safety measures to avoid reruns (thread?)
 *
 * So plans can be reused safely across different executions
 */
public final class OperatorExecution {

    // used to uniquely identify this execution
    public int id;

    public static final Random random = new Random();

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    IndexScanWithProjection operator){
        // build input

        List<Object> keyList = new ArrayList<>(operator.index.getColumns().length);
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.columnHash.contains( wherePredicate.columnReference.columnPosition )){
                keyList.add( wherePredicate.value );
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

        IKey inputKey = KeyUtils.buildInputKey(keyList.toArray());

        FilterContext filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);

//        return new OperatorExecution(random.nextInt(), filterContext, operator, inputKey );
        return operator.run( filterContext, inputKey );
    }



}
