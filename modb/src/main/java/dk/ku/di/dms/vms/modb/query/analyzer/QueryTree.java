package dk.ku.di.dms.vms.modb.query.analyzer;

import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Logical query plan tree
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
    public List<GroupByPredicate> groupByProjections;

    public List<ColumnReference> groupByColumns;

    // TODO order by predicate
    public List<OrderByPredicate> orderByPredicates;

    public QueryTree(List<WherePredicate> wherePredicates){
        this.wherePredicates = wherePredicates;
    }

    public QueryTree() {
        this.projections = new ArrayList<>(3);
        this.tables = new HashMap<>(2);
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>(3);
        this.groupByProjections = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.orderByPredicates = new ArrayList<>();
    }

    /**
      * This method allows the index selection to avoid a great number of combinations
      * on selecting an index. An index is stored with columns ordered.
      * So when we pass the columns of table involved in a query to the planner,
      * this goes already in order.
      * @param wherePredicate the predicate to add
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
     * @return whether it is a simple scan
     */
    public boolean isSimpleScan(){
        return isSingleTable() && this.joinPredicates.isEmpty() && this.groupByProjections.isEmpty() && this.orderByPredicates.isEmpty();
    }

    public boolean isSimpleAggregate(){
        return isSingleTable() && this.groupByProjections.size() == 1 && this.projections.isEmpty() && this.joinPredicates.isEmpty() && this.orderByPredicates.isEmpty();
    }

    public boolean isSimpleJoin(){
        return this.joinPredicates.size() == 1 && this.groupByProjections.isEmpty() && this.orderByPredicates.isEmpty();
    }

    /**
     * Also if this.groupByProjections.size() > 1
     */
    public boolean hasMultipleJoins(){
        return this.joinPredicates.size() >= 2;
    }

    public boolean isSingleTable(){
        return this.tables.size() == 1;
    }

}
