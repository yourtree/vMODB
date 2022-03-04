package dk.ku.di.dms.vms.database.query.planner.node.projection;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 *
 * This "operator" is simply taking a class and transforming the rows
 * contained in an {@link OperatorResult} into the class type
 */
public class Projector implements Supplier<Object>, Consumer<OperatorResult> {

    private final Class<?> clazz;

    private OperatorResult input;

    private final List<ColumnReference> projections;

    public Projector(final Class<?> clazz, final List<ColumnReference> projections){
        this.clazz = clazz;
        this.projections = projections;
    }

    @Override
    public void accept(OperatorResult operatorResult) {
        this.input = operatorResult;
    }

    @Override
    public Object get() {

        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor();

            // MethodHandle h = MethodHandles.lookup().unreflectSetter(field);
            MethodHandle[] setters = new MethodHandle[ projections.size() ];

            // TODO capture all of this on startup. get all calls to builder and then cache it
            // collect the setter
            int i = 0;
            for(final ColumnReference columnReference : projections){
                Field field = clazz.getField(columnReference.columnName);
                setters[columnReference.columnPosition] = MethodHandles.lookup().unreflectSetter( field );
                i++;
            }

            final Collection<Row> rows = input.get();

            if(rows.size() > 1) {

                List<Object> result = new ArrayList<>(rows.size());

                i = 0;
                for (final Row row : rows) {
                    final Object currObj = constructor.newInstance();

                    for (MethodHandle setter : setters) {
                        setter.invoke(currObj, row.get(i));
                        i++;
                    }

                    result.add(currObj);

                }

                return result;

            } else {
                // only one

                // necessary to use collection because of the map interface of underlying data structures
                final Row row = rows.iterator().next();

                final Object currObj = constructor.newInstance();

                i = 0;
                for (MethodHandle setter : setters) {
                    setter.invoke(currObj, row.get(i));
                    i++;
                }

                return currObj;

            }


        } catch (Throwable e) {
            e.printStackTrace();
        }

        return null;
    }
}
