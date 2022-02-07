package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.refac.Row;

import java.io.Serializable;

public class FilterBuilder {

    public static IFilter<Row> build(final WherePredicate whereClause) throws Exception {

        switch(whereClause.columnReference.column.type){

            case INT: {
                return getFilter( whereClause.expression, 1, (int) whereClause.value );
            }
            case STRING: {
                //return getFilter( whereClause.expression, (String) whereClause.value );
            }
            case CHAR: {
                //return getFilter( whereClause.expression, (Character) whereClause.value );
            }
            case LONG: {
                //return getFilter( whereClause.expression, (Long) whereClause.value );
            }
            case DOUBLE: {
                //return getFilter( whereClause.expression, (Double) whereClause.value );
            }
            default:
                throw new IllegalStateException("Unexpected value: " + whereClause.columnReference.column.type);
        }

    }

    interface Returnable<V extends Number> {
        V get(Row row, int columnIndex);
    }

    public static IFilter<Row> getFilter(
            final ExpressionEnum expression,
            final int columnIndex,
            final int fixedValue) throws Exception {

//        Returnable<Double> s1 = ( row, columnIndex2 ) -> row.getDouble(columnIndex2);
        Returnable<Double> s1 = Row::getDouble;

        switch(expression){
            case EQUALS:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) == 0;
                    }
                };
            case NOT_EQUALS:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) != 0;
                    }
                };
            case LESS_THAN_OR_EQUAL:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) <= 0;
                    }
                };
            case LESS_THAN:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) < 0;
                    }
                };
            case GREATER_THAN:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) > 0;
                    }
                };
            case GREATER_THAN_OR_EQUAL:
                return new Filter<Integer>(fixedValue, Integer::compareTo, columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        Integer value = row.getInt(this.columnIndex);
                        return this.comparator.compare( value, this.fixedValue ) >= 0;
                    }
                };
            case IS_NULL:
                return new NullFilter(columnIndex) {};
            case IS_NOT_NULL:
                return new NullFilter(columnIndex) {
                    @Override
                    public boolean test(Row row) {
                        return !super.test(row);
                    }
                };
            case LIKE: throw new Exception("Like does not apply to integer value.");
        }

        return null;
    }

}
