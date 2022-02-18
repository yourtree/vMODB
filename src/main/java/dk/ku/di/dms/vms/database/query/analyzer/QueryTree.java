package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;

/**
 *  Logical query plan tree.
  */
public class QueryTree {

    // projection
    public List<ColumnReference> projections;

    public Map<String,Table> tables;

    // join operations
    public List<JoinPredicate> joinPredicates;

    // predicates
    public List<WherePredicate> wherePredicates;

    public List<GroupByPredicate> groupByPredicates;

    // TODO order by predicate
    public List<OrderByPredicate> orderByPredicates;

    public QueryTree() {
        this.projections = new ArrayList<>();
        this.tables = new Hashtable<>();
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>();
        this.groupByPredicates = new ArrayList<>();
    }

    /**
     * This method allows the index selection to avoid a great number of combinations
     * on selecting an index. An index is stored with columns ordered.
     * So when we pass the columns of table involved in a query to the planner,
     * this goes already in order
     * @param wherePredicate
     */
    public void addWhereClauseSortedByColumnIndex( final WherePredicate wherePredicate ){

        Iterator<WherePredicate> it = wherePredicates.iterator();
        int posToInsert = 0;
        while (it.hasNext() &&
                wherePredicate.columnReference.columnIndex >
                        it.next().columnReference.columnIndex){
                posToInsert++;
        }

        this.wherePredicates.add(posToInsert, wherePredicate);

    }

}
