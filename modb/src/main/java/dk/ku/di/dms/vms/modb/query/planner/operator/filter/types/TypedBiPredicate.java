package dk.ku.di.dms.vms.modb.query.planner.operator.filter.types;

/**
 * Same as BiPredicate, but without the need to express a second typed parameter
 * Saving runtime resources
 * @param <T>
 */
@FunctionalInterface
public interface TypedBiPredicate<T> {

    boolean apply(T t1, T t2);

}
