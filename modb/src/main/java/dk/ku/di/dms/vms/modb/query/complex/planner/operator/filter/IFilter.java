package dk.ku.di.dms.vms.modb.query.complex.planner.operator.filter;

/**
 * Basic interface for filtering values of different types
 * See {@link FilterBuilder} to understand how these are built
 * @param <T>
 */
public interface IFilter<T> {

    default boolean eval(T x) {
        return false;
    }

    default boolean eval(T x, T y) {
        return false;
    }

    /*
     * For IN clause
     */
//    default boolean eval(T x, T... y){
//        return false;
//    }

}