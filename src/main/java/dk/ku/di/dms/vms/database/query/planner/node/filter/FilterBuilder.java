package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.clause.WhereClause;
import dk.ku.di.dms.vms.database.query.planner.node.filter.expr.FilterPredicate;
import dk.ku.di.dms.vms.database.query.planner.node.filter.expr.IFilterPredicate;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.lang.reflect.Field;
import java.util.Comparator;

public class FilterBuilder {

    public static IFilter<? extends AbstractEntity<?>> build(final WhereClause whereClause) {

        final Field field = whereClause.columnReference.column.field;

        switch(whereClause.columnReference.column.type){

            case INT: {
                final Comparator<Integer> comparator = new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1.compareTo(o2);
                    }
                };

                IFilterPredicate<Integer> predicate =
                        new FilterPredicate<>((Integer) whereClause.value, comparator);

                return new Filter<Integer>(field, predicate) {};

            }
            case STRING: {
                final Comparator<String> comparator = new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                };

                IFilterPredicate<String> predicate =
                        new FilterPredicate<>((String) whereClause.value, comparator);

                return new Filter<String>(field, predicate) {};

            }
            case CHAR: {

                Comparator<Character> comparator = new Comparator<Character>() {
                    @Override
                    public int compare(Character o1, Character o2) {
                        return o1.compareTo(o2);
                    }
                };

                IFilterPredicate<Character> predicate =
                        new FilterPredicate<>((Character) whereClause.value, comparator);

                return new Filter<Character>(field, predicate) {};

            }

            case LONG: {

                Comparator<Long> comparator = new Comparator<Long>() {
                    @Override
                    public int compare(Long o1, Long o2) {
                        return o1.compareTo(o2);
                    }
                };

                IFilterPredicate<Long> predicate =
                        new FilterPredicate<>((Long) whereClause.value, comparator);

                return new Filter<Long>(field, predicate) {};
            }

            case DOUBLE: {

                Comparator<Double> comparator = new Comparator<Double>() {
                    @Override
                    public int compare(Double o1, Double o2) {
                        return o1.compareTo(o2);
                    }
                };

                IFilterPredicate<Double> predicate =
                        new FilterPredicate<>((Double) whereClause.value, comparator);

                return new Filter<Double>(field, predicate) {};

            }

            default:
                throw new IllegalStateException("Unexpected value: " + whereClause.columnReference.column.type);
        }

    }

}
