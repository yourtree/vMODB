package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.api.modb.QueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.parser.stmt.JoinClauseElement;
import dk.ku.di.dms.vms.database.query.parser.stmt.SelectStatement;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.parser.stmt.UpdateStatement;
import dk.ku.di.dms.vms.database.store.Column;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

import java.util.List;
import java.util.Map;

import static dk.ku.di.dms.vms.database.query.parser.stmt.ExpressionEnum.EQUALS;


public class Analyzer {

    public final Catalog catalog;

    public Analyzer(Catalog catalog){
        this.catalog = catalog;
    }

    // basically transforms the raw input into known and safe metadata, e.g., whether a table, column exists
    public QueryTree parse(IStatement statement) throws Exception {

        QueryTree queryTree = new QueryTree();

        if(statement instanceof SelectStatement){

            // obtain the tables to look for the columns in projection first
            List<String> fromClause = ((SelectStatement) statement).fromClause;

            for(String tableStr : fromClause){
                Table table = catalog.tableMap.getOrDefault(tableStr,null);
                if(table != null) queryTree.tables.put(tableStr,table);
            }

            // join
            List<JoinClauseElement> joinClauseElements = ((SelectStatement) statement).joinClause;
            for(JoinClauseElement join : joinClauseElements){
                // TODO an optimization is iterating through the foreign keys of the table to find the match faster

                Table tableLeft = queryTree.tables.getOrDefault(join.tableLeft,null);
                if(tableLeft == null){
                    tableLeft = catalog.tableMap.getOrDefault(join.tableLeft,null);
                    if(tableLeft != null) queryTree.tables.put(join.tableLeft,tableLeft);
                }

                // find left column
                ColumnReference columnLeftReference = findColumnReference(join.columnRight, tableLeft);

                Table tableRight = queryTree.tables.getOrDefault(join.tableRight,null);
                if(tableRight == null){
                    tableRight = catalog.tableMap.getOrDefault(join.tableRight,null);
                    if(tableRight != null) queryTree.tables.put(join.tableRight,tableRight);
                }

                // find right column
                ColumnReference columnRightReference = findColumnReference(join.columnRight, tableRight);

                // build typed join clause
                JoinClause joinClause = new JoinClause(columnLeftReference, columnRightReference, join.expression, join.joinType);

                queryTree.joinClauses.add(joinClause);

            }

            // columns in projection may come from join
            List<String> columns = ((SelectStatement) statement).columns;
            // cannot allow same column name without AS from multiple tables
            for(String columnStr : columns){
                // TODO check if table name is included in string,
                //  that would make find the column ref faster
                ColumnReference columnReference = findColumnReference(columnStr, queryTree.tables);
                if(columnReference != null){
                    queryTree.columns.add(columnReference);
                }
            }

            // where
            // queryTree
            // TODO FINISH

        } else if(statement instanceof UpdateStatement){
            // TODO FINISH
        } else {
            throw new Exception("Unknown statement type.");
        }

        return null;

    }

    private ColumnReference findColumnReference(String columnStr, Map<String,Table> tables) {
        for(Table tbl : tables.values()){
            Column column = (Column) tbl.columnMap.get(columnStr);
            if(column != null){
                ColumnReference columnReference = new ColumnReference(column, tbl);
                return columnReference;
            }
        }
        return null;
    }

    private ColumnReference findColumnReference(String columnStr, Table table) {
        Column column = (Column) table.columnMap.get(columnStr);
        if(column != null){
            ColumnReference columnReference = new ColumnReference(column, table);
            return columnReference;
        }
        return null;
    }

}
