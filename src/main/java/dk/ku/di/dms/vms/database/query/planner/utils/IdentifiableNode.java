package dk.ku.di.dms.vms.database.query.planner.utils;

/**
 * Utility class to help with the identification and ordering of objects in lists or tree
 * Used in filters params and query plan tree
 * @param <T>
 */
public final class IdentifiableNode<T> {

    public final int id;
    public final T object;

    public IdentifiableNode(int id, T object) {
        this.id = id;
        this.object = object;
    }
}
