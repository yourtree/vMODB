package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.exception.MappingException;
import dk.ku.di.dms.vms.operational.DataOperationSignature;
import org.reflections.Configuration;
import org.reflections.Reflections;

import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.Index;
import javax.persistence.Table;

import static org.reflections.scanners.Scanners.FieldsAnnotated;

public class MetadataLoader {

    private static final Logger logger = LoggerFactory.getLogger(MetadataLoader.class);

    public ApplicationMetadata load(String forPackage) throws
            ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, MappingException {

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
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner(),
                        new MethodAnnotationsScanner(),
                        new FieldAnnotationsScanner()
                );

        Reflections reflections = new Reflections(reflectionsConfig);

        /*
         * Building catalog
         */

        // DatabaseMetadata databaseMetadata
        Catalog catalog = new Catalog();

        // get all fields annotated with column
        Set<Field> allEntityFields = reflections.get(
                FieldsAnnotated.with( Column.class )
                .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> map = allEntityFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                Collectors.toList()) );

        for(final Map.Entry<Class<?>,List<Field>> entry : map.entrySet()){



            Class<?> entity = entry.getKey();

            if( entity.getCanonicalName().contains("Product") ){



            List<Field> fields = entry.getValue();

            // build schema of each table

            final String[] columnNames = new String[fields.size()];
            final DataType[] columnDataTypes = new DataType[fields.size()];
            int i = 0;
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

                columnNames[i] = field.getName();

            }

            // check the indexes
            // https://www.baeldung.com/jpa-indexes

            Annotation[] annotations = entity.getAnnotations();
            for (Annotation an : annotations) {
                // the developer may need to use other annotations
                if(an instanceof Table){
                    Index[] indexes = ((Table) an).indexes();
                    for( Index index : indexes ){
                        String[] columnList = index.columnList().split(",\\s|,");
                        // TODO finish
                    }

                    // TODO get id annotation, which becomes the primary key index

                    break;
                }
            }

            Schema schema = new Schema(columnNames, columnDataTypes);


            // TODO finish. set pk and indexes
            }
        }

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
