package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.clause.JoinClause;
import dk.ku.di.dms.vms.database.query.analyzer.clause.WhereClause;
import dk.ku.di.dms.vms.database.query.parser.stmt.*;
import dk.ku.di.dms.vms.database.store.Column;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

import java.util.List;
import java.util.Map;

public final class Analyzer {

    public final Catalog catalog;

    public Analyzer(final Catalog catalog){
        this.catalog = catalog;
    }

    // basically transforms the raw input into known and safe metadata, e.g., whether a table, column exists
    public QueryTree analyze(final IStatement statement) throws Exception {

        final QueryTree queryTree = new QueryTree();

        if(statement instanceof SelectStatement){

            final SelectStatement select = (SelectStatement) statement;

            // from
            // obtain the tables to look for the columns in projection first
            List<String> fromClause = select.fromClause;

            for(String tableStr : fromClause){
                Table table = catalog.tableMap.getOrDefault(tableStr,null);
                if(table != null) queryTree.tables.put(tableStr,table);
            }

            // join
            List<JoinClauseElement> joinClauseElements = select.joinClause;
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

            // projection
            // columns in projection may come from join
            List<String> columns = select.columns;
            // cannot allow same column name without AS from multiple tables
            for(String columnStr : columns){
                // TODO check if table name is included in string,
                //  that would make find the column reference faster
                ColumnReference columnReference = findColumnReference(columnStr, queryTree.tables);
                if(columnReference != null){
                    queryTree.columns.add(columnReference);
                }
            }

            // where
            List<WhereClauseElement> where = select.whereClause;
            for(WhereClauseElement currWhere : where){

                // TODO column has table reference?
//                if( whereClauseElement.column.contains(".") ){
//
//                } else {
//
//                }

                ColumnReference columnReference = findColumnReference(currWhere.column, queryTree.tables);
                WhereClause whereClause;
                switch(columnReference.column.type){
                    case INT: whereClause =
                                new WhereClause<>(columnReference,currWhere.expression,(Integer)currWhere.value);
                                break;
                    case STRING: whereClause =
                                    new WhereClause<>(columnReference,currWhere.expression,(String)currWhere.value);
                                    break;
                    case CHAR: whereClause =
                                new WhereClause<>(columnReference,currWhere.expression,(Character) currWhere.value);
                                break;
                    case LONG: whereClause =
                                new WhereClause<>(columnReference,currWhere.expression,(Long) currWhere.value);
                                break;
                    case DOUBLE: whereClause =
                            new WhereClause<>(columnReference,currWhere.expression,(Double) currWhere.value);
                        break;
                    default: throw new Exception("Type not recognized");
                }

                queryTree.whereClauses.add(whereClause);

            }

            // TODO FINISH sort, group by

        } else if(statement instanceof UpdateStatement){

            final UpdateStatement select = (UpdateStatement) statement;

            // TODO FINISH

        } else {
            throw new Exception("Unknown statement type.");
        }

        return queryTree;

    }

    private ColumnReference findColumnReference(String columnStr, Map<String,? extends Table> tables) {
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
