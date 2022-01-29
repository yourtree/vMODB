package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.analyzer.clause.JoinClause;
import dk.ku.di.dms.vms.database.query.analyzer.clause.WhereClause;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryTree {

    public List<ColumnReference> columns;

    public Map<String,Table<?,?>> tables;

    public List<JoinClause> joinClauses;

    public List<WhereClause<?>> whereClauses;

    // TODO sort and group clauses

    public QueryTree() {
        this.columns = new ArrayList<>();
        this.tables = new HashMap<>();
        this.joinClauses = new ArrayList<>();
        this.whereClauses = new ArrayList<>();
    }

}
