package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.annotations.*;
import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.common.SimpleKey;
import dk.ku.di.dms.vms.database.store.index.UniqueHashIndex;
import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.meta.*;
import dk.ku.di.dms.vms.database.store.table.HashIndexedTable;
import dk.ku.di.dms.vms.database.store.table.Table;
import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.infra.AbstractEntity;
import dk.ku.di.dms.vms.metadata.exception.QueueMappingException;
import dk.ku.di.dms.vms.metadata.exception.NotAcceptableTypeException;
import dk.ku.di.dms.vms.metadata.exception.NoPrimaryKeyFoundException;
import dk.ku.di.dms.vms.operational.DataOperationSignature;
import org.reflections.Configuration;
import org.reflections.Reflections;

import org.reflections.scanners.*;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import static org.reflections.scanners.Scanners.FieldsAnnotated;

public class VmsMetadataLoader {

    private static final Logger logger = LoggerFactory.getLogger(VmsMetadataLoader.class);

    public VmsMetadata load(String forPackage) throws
            ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, QueueMappingException, NoPrimaryKeyFoundException, NotAcceptableTypeException, UnsupportedConstraint {

        final VmsMetadata vmsMetadata = new VmsMetadata();

        if(forPackage == null) {
            forPackage = "dk.ku.di.dms.vms";
        }
        // 1. create the data operations
        // 2. build the input to data operation and to output
        // it is necessary to analyze the code looking for @Transaction annotation
        // final Reflections reflections = new Reflections("dk.di.ku.dms.vms");

        Configuration reflectionsConfig = new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(forPackage)) //,
                .setScanners(
                        Scanners.SubTypes,
                        Scanners.TypesAnnotated,
                        Scanners.MethodsAnnotated,
                        Scanners.FieldsAnnotated
                );

        Reflections reflections = new Reflections(reflectionsConfig);

        /*
         * Building catalog
         */

        // DatabaseMetadata databaseMetadata
        final Catalog catalog = vmsMetadata.getCatalog();

        // get all fields annotated with column
        Set<Field> allEntityFields = reflections.get(
                FieldsAnnotated.with( Column.class )// .add(FieldsAnnotated.with(Id.class))
                .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by class
        Map<Class<?>,List<Field>> columnMap = allEntityFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                Collectors.toList()) );

        // get all fields annotated with column
        Set<Field> allPrimaryKeyFields = reflections.get(
                FieldsAnnotated.with( Id.class )
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> pkMap = allPrimaryKeyFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // get all foreign keys
        Set<Field> allAssociationFields = reflections.get(
                FieldsAnnotated.with(VmsForeignKey.class)
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> associationMap = allAssociationFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // table name and respective not parsed foreign keys
        Map<String, NotParsedForeignKeyReference[]> foreignKeysPerEntity = new HashMap<>();

        // build schema of each table
        // we build the schema in order to look up the fields and define the pk hash index
        for(final Map.Entry<Class<?>, List<Field>> entry : pkMap.entrySet()){

            Class<? extends AbstractEntity<?>> tableClass = (Class<? extends AbstractEntity<?>>) entry.getKey();
            List<Field> pkFields = entry.getValue();

            if(pkFields == null || pkFields.size() == 0){
                throw new NoPrimaryKeyFoundException("Table class "+tableClass.getCanonicalName()+" does not have a primary key.");
            }
            int totalNumberOfFields = pkFields.size();

            List<Field> foreignKeyFields = associationMap.get( tableClass );
            List<Field> columnFields = columnMap.get( tableClass );

            if(foreignKeyFields != null) {
                totalNumberOfFields += foreignKeyFields.size();
            }

            if(columnFields != null) {
                totalNumberOfFields += columnFields.size();
            }

            final String[] columnNames = new String[totalNumberOfFields];
            final DataType[] columnDataTypes = new DataType[totalNumberOfFields];

            int[] pkFieldsStr = new int[pkFields.size()];

            // iterating over pk columns
            int i = 0;
            for(final Field field : pkFields){
                Class<?> attributeType = field.getType();
                columnDataTypes[i] = getColumnDataTypeFromAttributeType(attributeType);
                pkFieldsStr[i] = i;
                columnNames[i] = field.getName();
                i++;
            }

            NotParsedForeignKeyReference[] foreignKeyReferences = null;
            if(foreignKeyFields != null) {
                foreignKeyReferences = new NotParsedForeignKeyReference[foreignKeyFields.size()];
                int j = 0;
                // iterating over association columns
                for (final Field field : foreignKeyFields) {

                    Class<?> attributeType = field.getType();
                    columnDataTypes[i] = getColumnDataTypeFromAttributeType(attributeType);
                    columnNames[i] = field.getName();
                    i++;

                    VmsForeignKey fkAnnotation = (VmsForeignKey) Arrays.stream(field.getAnnotations())
                            .filter(p -> p.annotationType() == VmsForeignKey.class).findFirst().get();

                    // later we parse into a Vms Table and check whether the types match
                    foreignKeyReferences[j] = new NotParsedForeignKeyReference(fkAnnotation.table(), fkAnnotation.column());
                    j++;
                }

            }

            // non-foreign key column constraints are inherent to the table, not referring to other tables
            ConstraintReference[] constraints = getConstraintReferences(columnFields, columnNames, columnDataTypes, i);

            Schema schema = new Schema(columnNames, columnDataTypes, pkFieldsStr, constraints);

            createVmsTableAndRespectiveIndexes(catalog, foreignKeysPerEntity, vmsMetadata, tableClass, foreignKeyReferences, schema);

        }

        parseForeignKeyReferences(catalog, foreignKeysPerEntity, vmsMetadata);

        // TODO finish --


        /* TODO look at this. we should provide this implementation
        SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
        SLF4J: Defaulting to no-operation (NOP) logger implementation
        SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
         */

        mapTransactionInputOutput(vmsMetadata, reflections);

        return vmsMetadata;

    }

    private ConstraintReference[] getConstraintReferences(List<Field> columnFields, String[] columnNames, DataType[] columnDataTypes, int columnPosition) throws NotAcceptableTypeException, UnsupportedConstraint {
        if(columnFields != null) {

            ConstraintReference[] constraints = null;

            // iterating over non-pk and non-fk columns;
            for (final Field field : columnFields) {

                Class<?> attributeType = field.getType();
                columnDataTypes[columnPosition] = getColumnDataTypeFromAttributeType(attributeType);

                // get constraints ought to be applied to this column, e.g., non-negative, not null, nullable
                List<Annotation> constraintAnnotations = Arrays.stream(field.getAnnotations())
                        .filter(p -> p.annotationType() == Positive.class ||
                                p.annotationType() == PositiveOrZero.class ||
                                p.annotationType() == NotNull.class ||
                                p.annotationType() == Null.class
                        ).collect(Collectors.toList());

                constraints = new ConstraintReference[constraintAnnotations.size()];
                int nC = 0;
                for (Annotation constraint : constraintAnnotations) {
                    String constraintName = constraint.annotationType().getName();
                    switch (constraintName) {
                        case "javax.validation.constraints.PositiveOrZero":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE_OR_ZERO, columnPosition);
                            break;
                        case "javax.validation.constraints.Positive":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE, columnPosition);
                            break;
                        case "javax.validation.constraints.Null":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NULL, columnPosition);
                            break;
                        case "javax.validation.constraints.NotNull":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NOT_NULL, columnPosition);
                            break;
                        default:
                            throw new UnsupportedConstraint("Constraint " + constraintName + " not supported.");
                    }
                    nC++;
                }

                columnNames[columnPosition] = field.getName();
                columnPosition++;
            }

            return constraints;

        }
        return null;
    }

    private void createVmsTableAndRespectiveIndexes(Catalog catalog,
                                                    Map<String, NotParsedForeignKeyReference[]> foreignKeysPerEntity,
                                                    // Map<Class<? extends AbstractEntity<?>>, Table> mapEntityClazzToTable,
                                                    VmsMetadata vmsMetadata,
                                                    Class<? extends AbstractEntity<?>> tableClass,
                                                    NotParsedForeignKeyReference[] foreignKeyReferences,
                                                    Schema schema) {
        Annotation[] annotations = tableClass.getAnnotations();
        for (Annotation an : annotations) {
            // the developer may need to use other annotations
            if(an instanceof VmsTable){

                // table name
                String tableName = ((VmsTable) an).name();
                HashIndexedTable table = new HashIndexedTable( tableName, schema);

                if (foreignKeyReferences != null) foreignKeysPerEntity.put( tableName, foreignKeyReferences);
                vmsMetadata.registerEntityClazzMapToTable(tableClass, table);

                // table indexes
                VmsIndex[] indexes = ((VmsTable) an).indexes();
                for( VmsIndex index : indexes ){
                    String[] columnList = index.columnList().split(",\\s|,");

                    // TODO finish
                    int[] columnPosArray = schema.buildColumnPositionArray( columnList );

                    IIndexKey logicalIndexKey;
                    IKey physicalIndexKey;
                    if(columnList.length > 1){
                        physicalIndexKey = new CompositeKey(columnPosArray);
                        int[] columnPosArrayInColumnPositionOrder = Arrays.stream(columnPosArray).sorted().toArray();
                        logicalIndexKey = new CompositeKey( columnPosArrayInColumnPositionOrder );
                    }else{
                        physicalIndexKey = new SimpleKey(columnPosArray[0]);
                        logicalIndexKey = new SimpleKey(columnPosArray[0]);
                    }

                    if(index.unique()){

                        // get column position in schema

                        UniqueHashIndex hashIndex = new UniqueHashIndex(table, columnPosArray);
                        table.addIndex( logicalIndexKey, physicalIndexKey, hashIndex );

                    } else if(index.range()) {
                        // no range indexes in JPA....
                    } else {
                        // btree...
                    }

                }

                catalog.insertTable(table);
                break; // the only important annotation is vms table, thus we can break the iteration
            }

        }
    }

    /**
     * after reading all entities, then I can parse the foreign keys to actual tables
     * the reason is that I need the references to all tables before reasoning about their associations
     * @param catalog
     * @param foreignKeysPerEntity
     * @param vmsMetadata
     */
    private void parseForeignKeyReferences(Catalog catalog,
                                           Map<String, NotParsedForeignKeyReference[]> foreignKeysPerEntity,
                                           VmsMetadata vmsMetadata) {

        for( final Map.Entry<String,NotParsedForeignKeyReference[]> entry : foreignKeysPerEntity.entrySet()){
            Table table = catalog.getTable( entry.getKey() );

            Map<Table, List<Integer>> foreignKeysGroupedByTableMap = new HashMap<>();

            // iterate and parse to table and column position
            Schema schema = table.getSchema();

            NotParsedForeignKeyReference[] notParsedForeignKeyReferences = entry.getValue();
            int size = notParsedForeignKeyReferences.length;

            for(int i = 0; i < size; i++){

                Table foreignTable = vmsMetadata.getTableByEntityClazz( notParsedForeignKeyReferences[i].entityClazz );

                int column = foreignTable.getSchema().getColumnPosition( notParsedForeignKeyReferences[i].columnName );


                if(foreignKeysGroupedByTableMap.get(foreignTable) == null){
                    List<Integer> fkList = new ArrayList<>();
                    fkList.add(column);
                    foreignKeysGroupedByTableMap.put(foreignTable, fkList);
                } else {
                    // add in order
                    Iterator<Integer> iterator = foreignKeysGroupedByTableMap.get(foreignTable).iterator();
                    int pos = 0;
                    while(iterator.hasNext() && column > iterator.next()){
                        pos++;
                    }
                    foreignKeysGroupedByTableMap.get(foreignTable).add( pos, column );
                }

            }

            Map<Table, int[]> finalMap = new HashMap<>(foreignKeysGroupedByTableMap.size());
            int[] columns;
            // now store as ordered array in the schema
            for( Map.Entry<Table, List<Integer>> entry2 : foreignKeysGroupedByTableMap.entrySet()){
                columns = new int[entry2.getValue().size()];
                for(int j = 0; j < entry2.getValue().size(); j++) {
                    columns[j] = entry2.getValue().get(j);
                }
                finalMap.put(entry2.getKey(),columns);
            }

            schema.addForeignKeyConstraints( finalMap );

        }
    }

    private class NotParsedForeignKeyReference{
        public final Class<? extends AbstractEntity<?>> entityClazz;
        public final String columnName;

        public NotParsedForeignKeyReference(Class<? extends AbstractEntity<?>> entityClazz, String columnName) {
            this.entityClazz = entityClazz;
            this.columnName = columnName;
        }
    }

    /**
     * Map transactions to input and output events
     */
    private void mapTransactionInputOutput(VmsMetadata vmsMetadata, Reflections reflections)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, QueueMappingException {

        Set<Method> transactionalMethods = reflections.getMethodsAnnotatedWith(Transactional.class);

        final Analyzer analyzer = new Analyzer(vmsMetadata.getCatalog());
        final Planner planner = new Planner();

        for (final Method method : transactionalMethods) {
            logger.info("Mapped = " + method.getName());

            String className = method.getDeclaringClass().getCanonicalName();
            Object obj = vmsMetadata.getMicroservice(className);

            if(obj == null){
                Class<?> cls = Class.forName(className);
                Constructor<?>[] constructors = cls.getDeclaredConstructors();
                Constructor<?> constructor = constructors[0];

                List<Object> repositoryList = new ArrayList<>();

                for (Class parameterType : constructor.getParameterTypes()) {
                    try {

                        RepositoryFacade facade = new RepositoryFacade(parameterType);

                        // vms metadata
                        facade.setVmsMetadata( vmsMetadata );

                        // instrumenting DBMS components
                        facade.setAnalyzer(analyzer);
                        facade.setPlanner( planner );

                        Object proxyInstance = Proxy.newProxyInstance(
                                VmsMetadataLoader.class.getClassLoader(),
                                new Class[]{parameterType},
                                // it works without casting as long as all services respect
                                // the constructor rule to have only repositories
                                facade);

                        repositoryList.add(proxyInstance);

                    } catch (Exception e){
                        logger.error(e.getMessage());
                    }
                }

                obj = constructor.newInstance(repositoryList.toArray());

                vmsMetadata.registerMicroservice(className,obj);
            }

            Class<?> outputType = method.getReturnType();

            List<Class<?>> inputTypes = new ArrayList<>();

            for(int i = 0; i < method.getParameters().length; i++){
                inputTypes.add(method.getParameters()[i].getType());
            }

            String[] inputQueues;

            Annotation[] ans = method.getAnnotations();
            DataOperationSignature dataOperation = new DataOperationSignature(obj, method);
            for (Annotation an : ans) {
                logger.info("Mapped = " + an.toString());
                if(an instanceof Inbound){
                    inputQueues = ((Inbound) an).values();
                    dataOperation.inputQueues = inputQueues;
                    for(int i = 0; i < inputQueues.length; i++) {

                        if(vmsMetadata.queueToEventMap.get(inputQueues[i]) == null){
                            vmsMetadata.queueToEventMap.put(inputQueues[i], (Class<IEvent>) inputTypes.get(i));
                        } else if( vmsMetadata.queueToEventMap.get(inputQueues[i]) != inputTypes.get(i) ) {
                            throw new QueueMappingException("Error mapping. An input queue cannot be mapped to two or more event types.");
                        }

                        List<DataOperationSignature> list = vmsMetadata.eventToOperationMap.get(inputQueues[i]);
                        if(list == null){
                            list = new ArrayList<>();
                            list.add(dataOperation);
                            vmsMetadata.eventToOperationMap.put(inputQueues[i], list);
                        } else {
                            list.add(dataOperation);
                        }
                    }
                }
                if(an instanceof Outbound){
                    String outputQueue = ((Outbound) an).value();
                    dataOperation.outputQueue = outputQueue;
                    // In the first design, the microservice cannot have two
                    // different operations reacting to the same event
                    // In other words, one event to an operation mapping.
                    // But one can achieve it by having two operations reacting
                    // to the same input event
                    vmsMetadata.queueToEventMap.put(outputQueue, (Class<IEvent>) outputType);
                }
            }

        }
    }

    private DataType getColumnDataTypeFromAttributeType(Class<?> attributeType) throws NotAcceptableTypeException {
        String attributeCanonicalName = attributeType.getCanonicalName();
        if (attributeCanonicalName.equalsIgnoreCase("int") || attributeType == Integer.class){
            return DataType.INT;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("float") || attributeType == Float.class){
            return DataType.FLOAT;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("double") || attributeType == Double.class){
            return DataType.DOUBLE;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("char") || attributeType == Character.class){
            return DataType.CHAR;
        }
        else if (attributeType == String.class){
            return DataType.STRING;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("long") || attributeType == Long.class){
            return DataType.LONG;
        }
        else if (attributeType == Date.class){
            return DataType.DATE;
        }
        else {
            throw new NotAcceptableTypeException(attributeType.getCanonicalName() + " is not accepted");
        }
    }


}
