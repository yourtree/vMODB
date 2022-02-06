package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;

import java.io.Serializable;

public class FilterBuilder {

    public static IFilter<? extends Serializable> build(final WherePredicate whereClause) throws Exception {

        switch(whereClause.columnReference.column.type){

            case INT: {
                return getFilter( whereClause.expression, (Integer) whereClause.value );
            }
            case STRING: {
                return getFilter( whereClause.expression, (String) whereClause.value );
            }
            case CHAR: {
                return getFilter( whereClause.expression, (Character) whereClause.value );
            }
            case LONG: {
                return getFilter( whereClause.expression, (Long) whereClause.value );
            }
            case DOUBLE: {
                return getFilter( whereClause.expression, (Double) whereClause.value );
            }
            default:
                throw new IllegalStateException("Unexpected value: " + whereClause.columnReference.column.type);
        }

    }

    public static IFilter<Double> getFilter(
            final ExpressionEnum expression,
            final Double fixedValue) throws Exception {
        return null;
    }

    public static IFilter<Long> getFilter(
            final ExpressionEnum expression,
            final Long fixedValue) throws Exception {
        return null;
    }

    public static IFilter<String> getFilter(
            final ExpressionEnum expression,
            final String fixedValue) throws Exception {
        return null;
    }

    public static IFilter<Character> getFilter(
            final ExpressionEnum expression,
            final Character fixedValue) throws Exception {
        return null;
    }

    public static IFilter<Integer> getFilter(
            final ExpressionEnum expression,
            final Integer fixedValue) throws Exception {

        switch(expression){
            case EQUALS:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) == 0;
                    }
                };
            case NOT_EQUALS:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) != 0;
                    }
                };
            case LESS_THAN_OR_EQUAL:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) <= 0;
                    }
                };
            case LESS_THAN:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) < 0;
                    }
                };
            case GREATER_THAN:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) > 0;
                    }
                };
            case GREATER_THAN_OR_EQUAL:
                return new Filter<Integer>(fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(Integer value) {
                        return this.comparator.compare( value, this.fixedValue ) >= 0;
                    }
                };
            case IS_NULL:
                return new NullFilter<Integer>() {};
            case IS_NOT_NULL:
                return new NullFilter<Integer>() {
                    @Override
                    public boolean test(Integer integer) {
                        return !super.test(integer);
                    }
                };
            case LIKE: throw new Exception("Like does not apply to integer value.");
        }

        return null;
    }

}
