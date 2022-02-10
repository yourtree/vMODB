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

    // TODO sort predicate
    public List<OrderByPredicate> orderByPredicates;

    public QueryTree() {
        this.projections = new ArrayList<>();
        this.tables = new Hashtable<>();
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>();
        this.groupByPredicates = new ArrayList<>();
    }
}
