package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.annotations.*;
import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.common.SimpleKey;
import dk.ku.di.dms.vms.database.store.index.HashIndex;
import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.meta.*;
import dk.ku.di.dms.vms.database.store.table.HashIndexedTable;
import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.exception.MappingException;
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

public class MetadataLoader {

    private static final Logger logger = LoggerFactory.getLogger(MetadataLoader.class);

    public ApplicationMetadata load(String forPackage) throws
            ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, MappingException, NoPrimaryKeyFoundException, NoAcceptableTypeException, UnsupportedConstraint {

        ApplicationMetadata applicationMetadata = new ApplicationMetadata();

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
        Catalog catalog = new Catalog();

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

        //  should we keep metadata about associations? the query processing is oblivious to that
        //      but, without enforcing cardinality of relationships, the developer can write erroneous code
        // get all associations
        Set<Field> allAssociationFields = reflections.get(
                FieldsAnnotated.with(OneToMany.class)
                        .add(FieldsAnnotated.with(ManyToOne.class))
                        .add(FieldsAnnotated.with(ManyToMany.class))
                        .add(FieldsAnnotated.with(OneToOne.class))
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> associationMap = allAssociationFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // build schema of each table
        // we build the schema in order to lookup the fields and define the pk hash index
        for(final Map.Entry<Class<?>,List<Field>> entry : pkMap.entrySet()){

            Class<?> tableClass = entry.getKey();
            List<Field> pkFields = entry.getValue();

            if(pkFields == null || pkFields.size() == 0){
                throw new NoPrimaryKeyFoundException("Table class "+tableClass.getCanonicalName()+" does not have a primary key.");
            }
            int totalNumberOfFields = pkFields.size();

            List<Field> associationFields = associationMap.get( tableClass );
            List<Field> columnFields = columnMap.get( tableClass );

            if(associationFields != null) {
                totalNumberOfFields += associationFields.size();
            } else {
                associationFields = Collections.emptyList();
            }

            if(columnFields != null) {
                totalNumberOfFields += columnFields.size();
            } else {
                columnFields = Collections.emptyList();
            }

            final String[] columnNames = new String[totalNumberOfFields];
            final DataType[] columnDataTypes = new DataType[totalNumberOfFields];

            int[] pkFieldsStr = new int[pkFields.size()];

            // iterating over pk columns
            int i = 0;
            for(final Field field : pkFields){
                Class<?> attributeType = field.getType();
                if (attributeType == Integer.class){
                    columnDataTypes[i] = DataType.INT;
                }
                else if (attributeType == Float.class){
                    columnDataTypes[i] = DataType.FLOAT;
                }
                else if (attributeType == Double.class){
                    columnDataTypes[i] = DataType.DOUBLE;
                }
                else if (attributeType == Character.class){
                    columnDataTypes[i] = DataType.CHAR;
                }
                else if (attributeType == String.class){
                    columnDataTypes[i] = DataType.STRING;
                }
                else if (attributeType == Long.class){
                    columnDataTypes[i] = DataType.LONG;
                }
                else {
                    throw new NoAcceptableTypeException( "Attribute "+field.getName()+ " type is not accepted." );
                }
                pkFieldsStr[i] = i;
                columnNames[i] = field.getName();
                i++;
            }

            // iterating over association columns
            for(final Field field : associationFields){

                // do we have a join column annotation ? @JoinColumn(name="checkout_id")
                Optional<Annotation> joinColumnAnnotation = Arrays.stream(field.getAnnotations())
                        .filter(p -> p.annotationType() == JoinColumn.class).findFirst();

                String columnNameJoin;
                if(joinColumnAnnotation.isPresent()){
                    columnNameJoin = ((JoinColumn) joinColumnAnnotation.get()).name();
                } else {

                    Optional<Annotation> joinColumnsAnnotation = Arrays.stream(field.getAnnotations())
                            .filter(p -> p.annotationType() == JoinColumns.class).findFirst();

                    // TODO finish joinColumnsAnnotation... for now simply getting field name

                    columnNameJoin = field.getName();
                }

                // FIXME the entity type may not be known at this time, so we defer the type definition to a later point
                //  storing the pending type definition
                //  <<the table class, the reference to the columnType array, the position>>
                columnDataTypes[i] = DataType.INT;
                columnNames[i] = columnNameJoin;
                i++;

            }

            ConstraintReference[] constraints = null;

            // iterating over non-pk and non-association columns;
            for(final Field field : columnFields){

                Class<?> attributeType = field.getType();
                if (attributeType == Integer.class || attributeType.getCanonicalName().equalsIgnoreCase("int")){
                    columnDataTypes[i] = DataType.INT;
                }
                else if (attributeType == Float.class){
                    columnDataTypes[i] = DataType.FLOAT;
                }
                else if (attributeType == Double.class){
                    columnDataTypes[i] = DataType.DOUBLE;
                }
                else if (attributeType == Character.class){
                    columnDataTypes[i] = DataType.CHAR;
                }
                else if (attributeType == String.class){
                    columnDataTypes[i] = DataType.STRING;
                }
                else if (attributeType == Long.class){
                    columnDataTypes[i] = DataType.LONG;
                }
                else {
                    throw new NoAcceptableTypeException( "Attribute "+field.getName()+ " type is not accepted." );
                }

                // get constraints ought to be applied to this column, e.g., non-negative, not null, nullable
                List<Annotation> constraintAnnotations = Arrays.stream(field.getAnnotations())
                        .filter(p -> p.annotationType() == Positive.class ||
                                     p.annotationType() == PositiveOrZero.class ||
                                     p.annotationType() == NotNull.class ||
                                     p.annotationType() == Null.class
                        ).collect(Collectors.toList());

                constraints = new ConstraintReference[constraintAnnotations.size()];
                int nC = 0;
                for(Annotation constraint : constraintAnnotations){
                    String constraintName = constraint.annotationType().getName();
                    switch(constraintName){
                        case "javax.validation.constraints.PositiveOrZero": constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE_OR_ZERO,i); break;
                        case "javax.validation.constraints.Positive": constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE,i); break;
                        case "javax.validation.constraints.Null" : constraints[nC] = new ConstraintReference(ConstraintEnum.NULL,i); break;
                        case "javax.validation.constraints.NotNull" : constraints[nC] = new ConstraintReference(ConstraintEnum.NOT_NULL,i); break;
                        default: throw new UnsupportedConstraint( "Constraint "+constraintName+" not supported." );
                    }
                    nC++;
                }

                columnNames[i] = field.getName();
                i++;
            }

            Schema schema;
            if(constraints != null) {
                schema = new Schema(columnNames, columnDataTypes, pkFieldsStr, constraints);
            } else {
                schema = new Schema(columnNames, columnDataTypes, pkFieldsStr);
            }

            Annotation[] annotations = tableClass.getAnnotations();
            for (Annotation an : annotations) {
                // the developer may need to use other annotations
                if(an instanceof VmsTable){

                    // table name
                    String tableName = ((VmsTable) an).name();
                    HashIndexedTable table = new HashIndexedTable( tableName, schema );

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
                            logicalIndexKey = new SimpleKey(columnPosArray[0]);;
                        }

                        if(index.unique()){

                            // get column position in schema

                            HashIndex hashIndex = new HashIndex(table, columnPosArray);
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


        // TODO finish --


        /* TODO look at this. we should provide this implementation
        SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
        SLF4J: Defaulting to no-operation (NOP) logger implementation
        SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
         */

        /*
         * Map transactions to input and output events
         */
        Set<Method> transactionalMethods = reflections.getMethodsAnnotatedWith(Transactional.class);

        Map<String,Object> loadedClasses = new HashMap<>();

        for (final Method method : transactionalMethods) {
            logger.info("Mapped = " + method.getName());

            String className = method.getDeclaringClass().getCanonicalName();
            Object obj = loadedClasses.get(className);
            if(obj == null){
                Class<?> cls = Class.forName(className);
                Constructor<?>[] constructors = cls.getDeclaredConstructors();
                Constructor<?> constructor = constructors[0];

                List<Object> repositoryList = new ArrayList<>();

                for (Class parameterType : constructor.getParameterTypes()) {
                    try {
                        Object proxyInstance = Proxy.newProxyInstance(
                                MetadataLoader.class.getClassLoader(),
                                new Class[]{parameterType},
                                // it works without casting as long as all services respect
                                // the constructor rule to have only repositories
                                new RepositoryFacade(parameterType));
                        repositoryList.add(proxyInstance);
                    } catch (Exception e){
                        logger.error(e.getMessage());
                    }
                }

                obj = constructor.newInstance(repositoryList.toArray());

                loadedClasses.put(className,obj);
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

                        if(applicationMetadata.queueToEventMap.get(inputQueues[i]) == null){
                            applicationMetadata.queueToEventMap.put(inputQueues[i], (Class<IEvent>) inputTypes.get(i));
                        } else if( applicationMetadata.queueToEventMap.get(inputQueues[i]) != inputTypes.get(i) ) {
                            throw new MappingException("Error mapping. An input queue cannot be mapped to two or more event types.");
                        }

                        List<DataOperationSignature> list = applicationMetadata.eventToOperationMap.get(inputQueues[i]);
                        if(list == null){
                            list = new ArrayList<>();
                            list.add(dataOperation);
                            applicationMetadata.eventToOperationMap.put(inputQueues[i], list);
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
                    applicationMetadata.queueToEventMap.put(outputQueue, (Class<IEvent>) outputType);
                }
            }

        }

        return applicationMetadata;

    }



}
