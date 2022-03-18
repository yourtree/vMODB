package dk.ku.di.dms.vms.database.query.planner.operator.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.store.meta.DataType;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.LIKE;

/**
 * A filter builder.
 */
public final class FilterBuilder {

    /**
     * The cache is progressively built (during application execution) instead of eagerly at startup
     */
    public static final Map<DataType, Map<ExpressionTypeEnum,IFilter<?>>> cachedFilters = new ConcurrentHashMap<>();

    public static IFilter<?> build(final WherePredicate wherePredicate) {

        DataType dataType = wherePredicate.columnReference.dataType;
        ExpressionTypeEnum expressionEnum = wherePredicate.expression;

        Map<ExpressionTypeEnum,IFilter<?>> filterDataTypeMap = cachedFilters.getOrDefault(dataType,null);
        if(filterDataTypeMap != null){
            if(filterDataTypeMap.get(expressionEnum) != null){
                return filterDataTypeMap.get(expressionEnum);
            }
        } else {
            filterDataTypeMap = new ConcurrentHashMap<>();
            cachedFilters.put( dataType, filterDataTypeMap );
        }

        IFilter<?> filter;

        switch(dataType){

            case INT: {
                filter = getFilter( wherePredicate.expression, Integer::compareTo );
                break;
            }
            case STRING: {

                if(wherePredicate.expression == LIKE){
                    // the default comparator interface does not allow taking advantage of specific type, since it is generic
                    filter = new IFilter<String>() {
                        public boolean eval(String v, String y) {
                            return v.contains(y);
                        }
                    };
                } else {
                    filter = getFilter( wherePredicate.expression, String::compareTo );
                }

                break;
            }
            case CHAR: {
                filter = getFilter( wherePredicate.expression, Character::compareTo );
                break;
            }
            case LONG: {
                filter = getFilter( wherePredicate.expression, Long::compareTo );
                break;
            }
            case DOUBLE: {
                filter = getFilter( wherePredicate.expression, Double::compareTo);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }

        filterDataTypeMap.put(expressionEnum,filter);
        return filter;

    }

    public static <V> IFilter<V> getFilter(
            final ExpressionTypeEnum expression,
            final Comparator<V> comparator) {

        switch(expression){
            case EQUALS:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) == 0;
                    }
                };
            case NOT_EQUALS:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) != 0;
                    }
                };
            case LESS_THAN_OR_EQUAL:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) <= 0;
                    }
                };
            case LESS_THAN:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) < 0;
                    }
                };
            case GREATER_THAN:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) > 0;
                    }
                };
            case GREATER_THAN_OR_EQUAL:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) >= 0;
                    }
                };
            case IS_NULL:
                return new IFilter<V>() {
                    public boolean eval(V v) {
                        return v == null;
                    }
                };
            case IS_NOT_NULL:
                return new IFilter<V>() {
                    public boolean eval(V v) {
                        return v != null;
                    }
                };
            default: // TODO log it appropriately
                throw new IllegalStateException("Unexpected condition.");
        }

    }

}
