package dk.ku.di.dms.vms.modb.query.analyzer;

import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.schema.ColumnReference;
import dk.ku.di.dms.vms.modb.table.Table;

import java.util.*;

/**
 *  Logical query plan tree.
  */
public class QueryTree {

    public Class<?> returnType;

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
        this.returnType = null;
        this.projections = new ArrayList<>();
        this.tables = new HashMap<>();
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>();
        this.groupByPredicates = new ArrayList<>();
    }

    public QueryTree(final Class<?> returnType) {
        this.returnType = returnType;
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
      * this goes already in order. TODO binary search
      * @param wherePredicate
      */
    public void addWhereClauseSortedByColumnIndex( final WherePredicate wherePredicate ){

        Iterator<WherePredicate> it = wherePredicates.iterator();
        int posToInsert = 0;
        while (it.hasNext() && wherePredicate.columnReference.columnPosition >
                                    it.next().columnReference.columnPosition){
                posToInsert++;
        }

        this.wherePredicates.add(posToInsert, wherePredicate);

    }

}
