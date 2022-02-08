package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.refac.Row;

import java.io.Serializable;

public class FilterBuilder {

    public static IFilter<?> build(final WherePredicate whereClause) throws Exception {

        switch(whereClause.columnReference.column.type){

            case INT: {
                return FilterBuilder.<Integer>getFilter( whereClause.expression, (int) whereClause.value );
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

    interface Returnable<V extends Serializable> {
        IFilter<V> get(V value, Comparable<V> comparable);
    }

    private static class EqualsPredicateComparator {
        public static <V> boolean compare( V x, V y ){
            return x == y;
        }
    }

    interface IComparator<V> {
        boolean compare(V x, V y);
        //boolean compare(V v);
    }

    interface IEqualsComparator<V> extends IComparator<V> {
        default boolean compare(V x, V y){
            return x == y;
        }
    }

    interface INotEqualsComparator<V> extends IComparator<V> {
        default boolean compare(V x, V y){
            return x != y;
        }
    }

    public static <V extends Serializable> IFilter<V> getFilter(
            final ExpressionEnum expression,
            final V fixedValue) throws Exception {

//        Returnable<Double> s1 = ( row, columnIndex2 ) -> row.getDouble(columnIndex2);
//        Returnable<Double> s1 = Row::getDouble;

        //EqualsPredicateComparator.<Integer> compare()

        //IEqualsPredicateComparator<V>.compare(  )
        IComparator<V> comp;
        IFilter<V> f;

        switch(expression){
            case EQUALS:
                comp = new IEqualsComparator<V>() {};
                f = new Filter<V>(fixedValue, 1, comp) {};
            case NOT_EQUALS:
                comp = new INotEqualsComparator<V>() {};
                //Returnable<V> s1 = ( value, comparable ) -> new Filter<V>(fixedValue, 1, comp) {};
                f = new Filter<V>(fixedValue, 1, comp) {};
            case LESS_THAN_OR_EQUAL:
//                return new Filter<Integer>(fixedValue, Integer::compareTo) {
//                    @Override
//                    public boolean test(Integer value) {
//                        return this.comparator.compare( value, this.fixedValue ) <= 0;
//                    }
//                };
            case LESS_THAN:
//                return new Filter<Integer>(fixedValue, Integer::compareTo) {
//                    @Override
//                    public boolean test(Integer value) {
//                        return this.comparator.compare( value, this.fixedValue ) < 0;
//                    }
//                };
            case GREATER_THAN:
//                return new Filter<Integer>(fixedValue, Integer::compareTo) {
//                    @Override
//                    public boolean test(Integer value) {
//                        return this.comparator.compare( value, this.fixedValue ) > 0;
//                    }
//                };
            case GREATER_THAN_OR_EQUAL:
//                return new Filter<Integer>(fixedValue, Integer::compareTo) {
//                    @Override
//                    public boolean test(Integer value) {
//                        return this.comparator.compare( value, this.fixedValue ) >= 0;
//                    }
//                };
            case IS_NULL:
//                return new NullFilter<Integer>() {};
            case IS_NOT_NULL:
//                return new NullFilter<Integer>() {
//                    @Override
//                    public boolean test(Integer value) {
//                        return !super.test(value);
//                    }
//                };
            case LIKE: throw new Exception("Like does not apply to integer value.");
            default: throw new Exception("Predicate not implemented");
        }

    }

}
