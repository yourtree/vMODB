package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.util.function.Predicate;

public interface IFilter<T> extends Predicate<T> {

    // public boolean apply(T entity) throws Throwable;

}
