package dk.ku.di.dms.vms.database.query.planner.node.filter;

/**
 * A dynbamic filter can be redefined, thus it can be reused in different queries
 * @param <V>
 */
public interface IDynamicFilter<V> extends IFilter<V> {

    /** to avoid creating again the same filter */
    void redefine(final V newFixedValue);

}
