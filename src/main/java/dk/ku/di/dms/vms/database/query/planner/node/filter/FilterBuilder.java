package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.meta.DataType;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A filter builder.
 */
public class FilterBuilder {

    /**
     * Only used if the same column is being checked in all where predicates.
     * For instance, x > 100 AND x < 200
     * Although not supported, range queries ( x BETWEEN y ) would also fit
     * @param wherePredicates
     * @return A composite filter, i.e., a set of filters applying to the same column
     * @throws Exception
     */
    public static IFilter<?> build(final List<WherePredicate> wherePredicates) throws Exception {

        IFilter<?> baseFilter = build( wherePredicates.get(0) );
        for(int i = 1; i < wherePredicates.size(); i++){
            baseFilter.and(build( wherePredicates.get(i) ));
        }

        return baseFilter;
    }

    public static IFilter<?> build(final WherePredicate wherePredicate) throws Exception {

        DataType dataType = wherePredicate.columnReference.dataType;

        switch(dataType){

            case INT: {
                return getFilter( wherePredicate.expression, dataType, (int) wherePredicate.value );
            }
            case STRING: {
                return getFilter( wherePredicate.expression, dataType, (String) wherePredicate.value );
            }
            case CHAR: {
                return getFilter( wherePredicate.expression, dataType, (Character) wherePredicate.value );
            }
            case LONG: {
                return getFilter( wherePredicate.expression, dataType, (Long) wherePredicate.value );
            }
            case DOUBLE: {
                return getFilter( wherePredicate.expression, dataType, (Double) wherePredicate.value );
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }

    }

    public static <V> IFilter<V> getFilter(
            final ExpressionEnum expression,
            final DataType dataType,
            final V fixedValue) throws Exception {

        /**
         * Only cast necessary. The ideal solution is to cache the reusable
         * filters created, so we avoid the overhead of filter creation for every query
         */
        Comparator<V> comp;

        switch(expression){
            case EQUALS:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) == 0;
                    }
                };
            case NOT_EQUALS:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) != 0;
                    }
                };
            case LESS_THAN_OR_EQUAL:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) <= 0;
                    }
                };
            case LESS_THAN:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) < 0;
                    }
                };
            case GREATER_THAN:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) > 0;
                    }
                };
            case GREATER_THAN_OR_EQUAL:
                comp = getComparator(dataType);
                return new Filter<V>(dataType, fixedValue, comp) {
                    @Override
                    public boolean test(V value) {
                        return this.comparator.compare( value, this.fixedValue ) >= 0;
                    }
                };
            case IS_NULL:
//                can be like this
//                return new IFilter<V>() {
//                    @Override
//                    public boolean test(V v) {
//                        return v == null;
//                    }
//                };
//                can also be like this
//                return v -> v == null;
                return Objects::isNull;
            case IS_NOT_NULL:
                return Objects::nonNull;
            case LIKE: throw new Exception("Like does not apply to integer value.");
            default: throw new Exception("Predicate not implemented");
        }

    }

    // TODO test this comparator later
    private static <V> Comparator<V> getComp(){
        return (Comparator<V>) new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return (x < y) ? -1 : ((x == y) ? 0 : 1);
            }
        };
    }

    private static Comparator getComparator(DataType type) {
        switch (type) {
            case INT: return (Comparator<Integer>) Integer::compareTo;
            case LONG: return (Comparator<Long>) Long::compareTo;
            case DOUBLE: return (Comparator<Double>) Double::compareTo;
            case CHAR: return (Comparator<Character>) Character::compareTo;
            case STRING: return (Comparator<String>) String::compareTo;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

}
