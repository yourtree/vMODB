package dk.ku.di.dms.vms.modb.query.analyzer;

import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;

import java.util.*;

/**
 *  Logical query plan tree.
  */
public class QueryTree {

    // projection
    public List<ColumnReference> projections;

    // store all tables included in this query, including those not participating in any join
    public Map<String, Table> tables;

    // join operations
    public List<JoinPredicate> joinPredicates;

    // predicates
    public List<WherePredicate> wherePredicates;

    // aggregate operations
    public List<GroupByPredicate> groupByPredicates;

    // TODO order by predicate
    public List<OrderByPredicate> orderByPredicates;

    /**
     * No explicit return type. Must be used for testing-purpose solely,
     * since this DBMS is an application-focused DBMS
     */
    public QueryTree() {
        this.projections = new ArrayList<>();
        this.tables = new HashMap<>();
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>();
        this.groupByPredicates = new ArrayList<>();
    }

    /**
      * This method allows the index selection to avoid a great number of combinations
      * on selecting an index. An index is stored with columns ordered.
      * So when we pass the columns of table involved in a query to the planner,
      * this goes already in order.
      * @param wherePredicate
      */
    public void addWhereClauseSortedByColumnIndex( WherePredicate wherePredicate ){

        int size = wherePredicates.size();
        if(size == 0){
            this.wherePredicates.add(wherePredicate);
            return;
        }

        if(size == 1){
            if(wherePredicate.columnReference.columnPosition >
                    wherePredicates.get(0).columnReference.columnPosition) {
                this.wherePredicates.add(1,wherePredicate);
            } else {
                this.wherePredicates.add(0, wherePredicate);
            }
            return;
        }

        // if size >= 2 then use binary search

        int half;

        int start = 0, end = size - 1;

        do {

            half = ((end + start) / 2);

            if(wherePredicates.get(half).columnReference.columnPosition >
                    wherePredicate.columnReference.columnPosition){
                end = half; // always guarantee end is within bounds
            } else {
                start = half;// always guarantee start is within bounds
            }

        } while(start != end);

        if(wherePredicates.get(half).columnReference.columnPosition <
                wherePredicate.columnReference.columnPosition){
            half++;
        }

        this.wherePredicates.add(half, wherePredicate);

    }

    /**
     * Simple select if no joins, grouping, and sort are present
     * @return
     */
    public boolean isSimpleSelect(){
        return joinPredicates.isEmpty() && groupByPredicates.isEmpty() && orderByPredicates.isEmpty();
    }

    public boolean isSimpleAggregate(){
        return !groupByPredicates.isEmpty() && joinPredicates.isEmpty()  && orderByPredicates.isEmpty();
    }

    public boolean isSimpleJoin(){
        return !joinPredicates.isEmpty() && groupByPredicates.isEmpty()  && orderByPredicates.isEmpty();
    }

}
