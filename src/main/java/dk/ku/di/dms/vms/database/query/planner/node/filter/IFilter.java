package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.util.function.Predicate;

public interface IFilter<T> extends Predicate<T> {

    default IFilter<T> and(Predicate<? super T> other) {
        return (t) -> test(t) && other.test(t);
    }


}