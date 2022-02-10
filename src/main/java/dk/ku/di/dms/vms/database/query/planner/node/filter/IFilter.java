package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.util.function.Predicate;

public interface IFilter<T> extends Predicate<T> {

    /**
     * We know it is safe because the planner makes sure to apply
     * only filters of the same type
     * @param other the other filter to include
     * @return lambda approach to combine different filters
     */
    @SuppressWarnings("unchecked")
    default IFilter<T> and(IFilter other) {
        return (t) -> test(t) && other.test(t);
    }

}