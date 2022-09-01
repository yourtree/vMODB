package dk.ku.di.dms.vms.modb.query.planner.execution;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Encapsulates the mutating aspects of a plan (input and filters)
 * as well as providing a set of safety measures to avoid reruns (thread?)
 *
 * So plans can be reused safely across different executions
 */
public class OperatorExecution {

    // used to uniquely identify this execution
    public final int id;

    public final IKey[] inputKeys;

    public final FilterContext filterContext;

    public final AbstractOperator operator;

    public static final Random random = new Random();

    // open for tests? maybe
    public OperatorExecution(int id,
                             FilterContext filterContext,
                             AbstractOperator operator,
                             IKey... inputKeys) {
        this.id = id;
        this.inputKeys = inputKeys;
        this.filterContext = filterContext;
        this.operator = operator;
    }

    public static OperatorExecution build(List<WherePredicate> wherePredicates,
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

        return new OperatorExecution(
                random.nextInt(), filterContext, operator, inputKey );

    }


}
