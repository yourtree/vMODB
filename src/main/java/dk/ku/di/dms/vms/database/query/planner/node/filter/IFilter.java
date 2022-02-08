package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.io.Serializable;
import java.util.function.Predicate;

public interface IFilter<T extends Serializable> extends Predicate<T> {

    default IFilter<T> and(IFilter other) {
        return (t) -> test(t) && other.test(t);
    }

}