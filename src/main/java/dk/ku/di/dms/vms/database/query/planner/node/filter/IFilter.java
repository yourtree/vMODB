package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.util.function.Predicate;

public interface IFilter<T> extends Predicate<T> {

    /**
     * Lack of parameter
     * @param other the other filter to include
     * @return lambda approach to combine different filters
     */
    default IFilter<T> and(IFilter other) {
        return (t) -> test(t) && other.test(t);
    }

}