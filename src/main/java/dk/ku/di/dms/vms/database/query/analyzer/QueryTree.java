package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryTree {

    // projection
    public List<ColumnReference> projections;

    public Map<String,Table<?,?>> tables;

    // join operations
    public List<JoinPredicate> joinOperations;

    // predicates
    public List<WherePredicate> wherePredicates;

    // TODO sort and group clauses

    public QueryTree() {
        this.projections = new ArrayList<>();
        this.tables = new HashMap<>();
        this.joinOperations = new ArrayList<>();
        this.wherePredicates = new ArrayList<>();
    }

}
