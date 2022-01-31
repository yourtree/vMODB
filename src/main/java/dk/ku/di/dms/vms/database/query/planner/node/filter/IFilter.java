package dk.ku.di.dms.vms.database.query.planner.node.filter;

public interface IFilter<T> {

    public boolean apply(T entity) throws IllegalAccessException;

}
