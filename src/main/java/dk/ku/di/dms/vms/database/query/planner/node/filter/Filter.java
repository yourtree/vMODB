package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Comparator;

public abstract class Filter<V extends Serializable> implements IFilter<AbstractEntity<?>> {

    protected final Field field;

    protected final MethodHandle h;

    public final V fixedValue;

    public final Comparator<V> comparator;

    public Filter(final Field field, final V fixedValue, final Comparator<V> comparator) throws IllegalAccessException {
        this.field = field;
        this.fixedValue = fixedValue;
        this.comparator = comparator;
        // TODO should I call lookup on every constructor? or can I have just one and pass as parameter?
        this.h = MethodHandles.lookup().unreflectGetter(this.field);
    }

}
