package dk.ku.di.dms.vms.database.query.planner.node.filter;

public interface IFilter<T> {

    default boolean eval(T x) {
        return false;
    }

    default boolean eval(T x, T y) {
        return false;
    }

    default boolean eval(T x, T... y){
        return false;
    }

}