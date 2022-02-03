package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.clause.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.planner.node.filter.expr.FilterPredicate;
import dk.ku.di.dms.vms.database.query.planner.node.filter.expr.IFilterPredicate;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.lang.reflect.Field;

public class FilterBuilder {

    public static IFilter<? extends AbstractEntity<?>> build(final WherePredicate whereClause) throws IllegalAccessException {

        final Field field = whereClause.columnReference.column.field;

        switch(whereClause.columnReference.column.type){

            case INT: {
                // IFilterPredicate<Integer> predicate = new FilterPredicate<>((Integer) whereClause.value, Integer::compareTo);
                return getIntegerFilter( whereClause.expression, field, (Integer) whereClause.value );
            }
            case STRING: {

                IFilterPredicate<String> predicate = new FilterPredicate<>((String) whereClause.value, String::compareTo);

                //return new Filter<String>(field, predicate) {};

            }
            case CHAR: {
                IFilterPredicate<Character> predicate = new FilterPredicate<>((Character) whereClause.value, Character::compareTo);
                //return new Filter<Character>(field, predicate) {};
            }

            case LONG: {
                IFilterPredicate<Long> predicate = new FilterPredicate<>((Long) whereClause.value, Long::compareTo);
                //return new Filter<Long>(field, predicate) {};
            }

            case DOUBLE: {
                IFilterPredicate<Double> predicate = new FilterPredicate<>((Double) whereClause.value, Double::compareTo);
                //return new Filter<Double>(field, predicate) {};
            }

            default:
                throw new IllegalStateException("Unexpected value: " + whereClause.columnReference.column.type);
        }


    }

    public static Filter<Integer> getIntegerFilter(
            final ExpressionEnum expression,
            final Field field, final Integer fixedValue) throws IllegalAccessException {


        switch(expression){
            case EQUALS:
                return new Filter<Integer>(field, fixedValue, Integer::compareTo) {
                    @Override
                    public boolean test(AbstractEntity<?> entity) {
                        Integer value = null;
                        try {
                            value = (Integer) this.h.invoke( entity );
                        } catch (Throwable e) {
                            e.printStackTrace();
                            return false;
                        }
                        int res = this.comparator.compare( value, this.fixedValue );
                        return res == 0;
                    }
                };
        }

        return null;
    }

}
