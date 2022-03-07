package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.annotations.*;
import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.common.SimpleKey;
import dk.ku.di.dms.vms.database.store.index.HashIndex;
import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
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

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import static org.reflections.scanners.Scanners.FieldsAnnotated;

public class MetadataLoader {

    private static final Logger logger = LoggerFactory.getLogger(MetadataLoader.class);

    public ApplicationMetadata load(String forPackage) throws
            ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, MappingException, NoPrimaryKeyFoundException, NoAcceptableTypeException {

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
        Map<Class<?>,List<Field>> map = allEntityFields.stream()
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

        for(final Map.Entry<Class<?>,List<Field>> entry : map.entrySet()){

            Class<?> tableClass = entry.getKey();
            List<Field> fields = entry.getValue();

            // build schema of each table
            // we build the schema in order to lookup the fields and define the pk hash index
            List<Field> pkFields = pkMap.get( tableClass );

            if(pkFields == null || pkFields.size() == 0){
                throw new NoPrimaryKeyFoundException("Table class "+tableClass.getCanonicalName()+" does not have a primary key.");
            }

            int totalNumberOfFields = pkFields.size() + fields.size();

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

            // iterating over non-pk columns
            // int i = pkFields.size();
            for(final Field field : fields){

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

                columnNames[i] = field.getName();
                i++;
            }

            Schema schema = new Schema(columnNames, columnDataTypes, pkFieldsStr);

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
                        } else {
                            throw new MappingException("Error mapping. An input queue cannot be mapped to tow or more event types.");
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
