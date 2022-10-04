//package dk.ku.di.dms.vms.modb.query.complex.planner.operator.projection;
//
//import dk.ku.di.dms.vms.modb.common.interfaces.result.DataTransferObjectOperatorResult;
//import dk.ku.di.dms.vms.modb.common.interfaces.result.RowOperatorResult;
//import dk.ku.di.dms.vms.modb.common.interfaces.db.IOperatorResult;
//import dk.ku.di.dms.vms.modb.definition.ColumnReference;
//import dk.ku.di.dms.vms.modb.definition.Row;
//
//import java.lang.invoke.MethodHandle;
//import java.lang.invoke.MethodHandles;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Field;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.List;
//import java.util.function.Consumer;
//import java.util.function.Supplier;
//
///**
// *
// * This "operator" is simply taking a class and transforming the rows
// * contained in an {@link RowOperatorResult} into the class type
// */
//public class TypedProjector implements Supplier<IOperatorResult>, Consumer<IOperatorResult> {
//
//    private final Class<?> clazz;
//
//    private RowOperatorResult input;
//
//    private final List<ColumnReference> projections;
//
//    public TypedProjector(final Class<?> clazz, final List<ColumnReference> projections){
//        this.clazz = clazz;
//        this.projections = projections;
//    }
//
//    @Override
//    public void accept(IOperatorResult operatorResult) {
//        this.input = operatorResult.asRowOperatorResult();
//    }
//
//    @Override
//    public IOperatorResult get() {
//
//        try {
//            Constructor<?> constructor = clazz.getDeclaredConstructor();
//
//            MethodHandle[] setters = new MethodHandle[ projections.size() ];
//
//            // TODO capture all of this on startup. get all calls to builder and then cache it
//            // collect the setter
//            int i = 0;
//            for(final ColumnReference columnReference : projections){
//                Field field = clazz.getField(columnReference.columnName);
//                setters[i] = MethodHandles.lookup().unreflectSetter( field );
//                i++;
//            }
//
//            final Collection<Row> rows = input.getRows();
//
//            if(rows.size() > 1) {
//
//                List<Object> result = new ArrayList<>(rows.size());
//
//                i = 0;
//                for (final Row row : rows) {
//                    final Object currObj = constructor.newInstance();
//
//                    for (MethodHandle setter : setters) {
//                        setter.invoke(currObj, row.get( projections.get( i ).columnPosition ));
//                        i++;
//                    }
//
//                    result.add(currObj);
//
//                }
//
//                return new DataTransferObjectOperatorResult(result);
//
//            } else {
//                // only one
//
//                // necessary to use collection because of the map interface of underlying data structures
//                final Row row = rows.iterator().next();
//
//                final Object currObj = constructor.newInstance();
//
//                i = 0;
//                for (MethodHandle setter : setters) {
//                    setter.invoke(currObj, row.get( projections.get( i ).columnPosition ));
//                    i++;
//                }
//
//                return new DataTransferObjectOperatorResult(Collections.singletonList(currObj));
//
//            }
//
//
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
//}
