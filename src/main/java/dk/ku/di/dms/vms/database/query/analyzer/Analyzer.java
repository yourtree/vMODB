package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.*;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.List;
import java.util.Map;

/**
 * Class responsible for analyzing a statement {@link IStatement}
 */
public final class Analyzer {

    private final Catalog catalog;

    public Analyzer(final Catalog catalog){
        this.catalog = catalog;
    }

    public QueryTree analyze(final IStatement statement, final Class<?> clazz) throws AnalyzerException {
        final QueryTree queryTree = new QueryTree(clazz);
        return analyze( statement, queryTree );
    }

    public QueryTree analyze(final IStatement statement) throws AnalyzerException {
        final QueryTree queryTree = new QueryTree();
        return analyze( statement, queryTree );
    }

    // basically transforms the raw input into known and safe metadata, e.g., whether a table, column exists
    private QueryTree analyze(final IStatement statement, final QueryTree queryTree) throws AnalyzerException {

        /**
         * https://docs.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql?view=sql-server-ver15#logical-processing-order-of-the-select-statement
         */
        if(statement instanceof SelectStatement){

            final SelectStatement select = (SelectStatement) statement;

            // from
            // obtain the tables to look for the columns in projection first
            List<String> fromClause = select.fromClause;

            for(String tableStr : fromClause){
                Table table = catalog.getTable(tableStr);
                if(table != null) queryTree.tables.put(tableStr,table);
            }

            // join
            if(select.joinClause != null) {
                List<JoinClauseElement> joinClauseElements = select.joinClause;
                for (JoinClauseElement join : joinClauseElements) {
                    // TODO an optimization is iterating through the foreign keys of the table to find the match faster

                    Table tableLeft = queryTree.tables.getOrDefault(join.tableLeft, null);

                    // find in catalog if not found in query yet
                    if (tableLeft == null) {
                        tableLeft = catalog.getTable(join.tableLeft);
                        if (tableLeft != null) {
                            queryTree.tables.put(join.tableLeft, tableLeft);
                        } else {
                            throw new AnalyzerException("Unknown " + join.tableLeft + " table.");
                        }
                    }

                    // find left column
                    ColumnReference columnLeftReference = findColumnReference(join.columnLeft, tableLeft);

                    Table tableRight = queryTree.tables.getOrDefault(join.tableRight, null);
                    if (tableRight == null) {
                        tableRight = catalog.getTable(join.tableRight);
                        if (tableRight != null) {
                            queryTree.tables.put(join.tableRight, tableRight);
                        } else {
                            throw new AnalyzerException("Unknown " + join.tableRight + " table.");
                        }
                    }

                    // find right column
                    ColumnReference columnRightReference = findColumnReference(join.columnRight, tableRight);

                    // build typed join clause
                    JoinPredicate joinClause = new JoinPredicate(columnLeftReference, columnRightReference, join.expression, join.joinType);

                    queryTree.joinPredicates.add(joinClause);

                }
            }

            // projection
            // columns in projection may come from join
            List<String> columns = select.selectClause;
            // cannot allow same column name without AS from multiple tables
            for(String columnRefStr : columns){
                if( columnRefStr.contains(".") ){
                    String[] splitted = columnRefStr.split("\\."); // FIXME check if there are 2 indexes in array
                    ColumnReference columnReference = findColumnReference(splitted[1], splitted[0], queryTree.tables);
                    queryTree.projections.add(columnReference);
                } else {
                    ColumnReference columnReference = findColumnReference(columnRefStr, queryTree.tables);
                    queryTree.projections.add(columnReference);
                }

            }

            // where
            if(select.whereClause != null) {
                List<WhereClauseElement<?>> where = select.whereClause;
                for (WhereClauseElement<?> currWhere : where) {

                    if (currWhere.value == null) {
                        throw new AnalyzerException("Parameter of where clause cannot be null value");
                    }

                    ColumnReference columnReference;

                    String tableName;
                    String columnName;

                    if (currWhere.column.contains(".")) {
                        String[] split = currWhere.column.split("\\.");
                        tableName = split[0];
                        columnName = split[1];
                        Table table = queryTree.tables.getOrDefault(tableName, null);
                        if (table != null) {
                            columnReference = findColumnReference(columnName, table);
                        } else {
                            throw new AnalyzerException("Table not defined in the query: " + tableName);
                        }
                    } else {
                        columnReference = findColumnReference(currWhere.column, queryTree.tables);
                    }

                    // is it a reference to a table or a char? e.g., "'something'"
                    // FIXME for now I am considering all strings are joins

                    // check if there is some inner join. i.e., the object is a literal?
                    if (currWhere.value instanceof String) {

                        ColumnReference columnReference1;
                        String value = (String) currWhere.value;

                        if (value.contains(".")) {
                            // <table>.<column>
                            String[] split = value.split("\\.");
                            tableName = split[0];
                            columnName = split[1];
                            Table table = queryTree.tables.getOrDefault(tableName, null);
                            if (table != null) {
                                columnReference1 = findColumnReference(columnName, table);
                            } else {
                                throw new AnalyzerException("Table not defined in the query: " + tableName);
                            }
                        } else {
                            columnReference1 = findColumnReference(currWhere.column, queryTree.tables);
                        }

                        // build typed join clause
                        JoinPredicate joinClause = new JoinPredicate(columnReference, columnReference1, currWhere.expression, JoinTypeEnum.INNER_JOIN);

                        queryTree.joinPredicates.add(joinClause);

                    } else {
                        // simple where
                        WherePredicate whereClause = new WherePredicate(columnReference, currWhere.expression, currWhere.value);

                        // The order of the columns declared in the index definition matters
                        queryTree.addWhereClauseSortedByColumnIndex(whereClause);
                    }

                }
            }

            // TODO FINISH sort and group by

        } else if(statement instanceof UpdateStatement){

            final UpdateStatement select = (UpdateStatement) statement;

            // TODO FINISH

        } else {
            throw new AnalyzerException("Unknown statement type.");
        }

        return queryTree;

    }

    private ColumnReference findColumnReference(String columnStr, Map<String,Table> tables) throws AnalyzerException {

        ColumnReference columnReferenceToResult = null;

        for(Table table : tables.values()){
            final Schema schema = table.getSchema();
            final Integer columnIndex = schema.getColumnPosition(columnStr);
            if(columnIndex != null) {
                if(columnReferenceToResult == null) {
                    columnReferenceToResult = new ColumnReference(columnStr, columnIndex, table);
                } else {
                    throw new AnalyzerException("Cannot refer to a column name that appear in more than a table without proper reference in the query");
                }
            }
        }
        if(columnReferenceToResult != null) {
            return columnReferenceToResult;
        }
        throw new AnalyzerException("Column " + columnStr +" does not exist in the catalog of tables");
    }

    private ColumnReference findColumnReference(String columnStr, Table table) throws AnalyzerException {
        final Schema schema = table.getSchema();
        Integer columnIndex = schema.getColumnPosition(columnStr);
        if(columnIndex == null){
            throw new AnalyzerException("Column does not exist in the table");
        }
        return new ColumnReference(columnStr, columnIndex, table);
    }

    /**
     * Another strategy is giving to the constructor of ColumnReference the duty to check whether the columnIndex exists
     * This way we avoid checking == null everytime.
     * @param columnStr
     * @param tableStr
     * @param tables
     * @return
     * @throws AnalyzerException
     */
    private ColumnReference findColumnReference(String columnStr, String tableStr, Map<String,Table> tables) throws AnalyzerException {

        if(tables.get(tableStr) == null ){
            throw new AnalyzerException("Table "+ tableStr + " does not exist in the catalog.");
        }
        final Table table = tables.get(tableStr);
        final Schema schema = table.getSchema();
        Integer columnIndex = schema.getColumnPosition(columnStr);
        if(columnIndex == null){
            throw new AnalyzerException("Column does not exist in the table "+ tableStr);
        }
        return new ColumnReference(columnStr, columnIndex, table);
    }

}
