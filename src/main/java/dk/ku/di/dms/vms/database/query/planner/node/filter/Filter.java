package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.function.Predicate;

public abstract class Filter<V extends Serializable> implements IFilter<AbstractEntity<?>> {

    private final Field field;

    private final Predicate<V> predicate;

    public Filter(final Field field, Predicate<V> predicate){
        this.field = field;
        this.predicate = predicate;
    }

    public boolean apply(AbstractEntity<?> entity) throws IllegalAccessException {

        // TODO how can I access a column value without reflection? i would need to
        //  store the fields in the table definition somehow to decrease the cost of accessing the fled dynamically
        // yes, but I need to change the metadata loader for that
        // Field field = entity.getClass().getField("c_id");

        V value = (V) this.field.get( entity );
        return this.predicate.test( value );
    }

}
