package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.H2RepositoryFacade;
import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.exception.MappingException;
import dk.ku.di.dms.vms.operational.DataOperationSignature;
import org.reflections.Reflections;

import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataLoader {

    private static Logger logger = LoggerFactory.getLogger(MetadataLoader.class);

    public ApplicationMetadata load(String forPackage) throws
            ClassNotFoundException, InvocationTargetException, InstantiationException,
            IllegalAccessException, MappingException {

        ApplicationMetadata config = new ApplicationMetadata();

        if(forPackage == null) forPackage = "dk.ku.di.dms.vms";
        // 1. create the data operations
        // 2. build the input to data operation and to output
        // it is necessary to analyze the code looking for @Transaction annotation
        // final Reflections reflections = new Reflections("dk.di.ku.dms.vms");

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(forPackage)) //,
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner(),
                        new MethodAnnotationsScanner(),
                        new FieldAnnotationsScanner()
                )
        );

        Set<Method> set = reflections.getMethodsAnnotatedWith(Transactional.class);

        Map<String,Object> loadedClasses = new HashMap<>();

        for (Method method : set) {
            logger.info("Mapped = " + method.getName());

            String className = method.getDeclaringClass().getCanonicalName();
            Object obj = loadedClasses.get(className);
            if(obj == null){
                Class cls = Class.forName(className);
                Constructor<?>[] constructors = cls.getDeclaredConstructors();
                Constructor ctor = constructors[0];

                //            Constructor ctor = cls.getDeclaredConstructor(IRepository.class);
                //            IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
                //                    AppTest.class.getClassLoader(),
                //                    new Class[] { IProductRepository.class },
                //                    new H2RepositoryFacade( IProductRepository.class ));

                List<Object> repositoryList = new ArrayList<>();

                for (Class parameterType : ctor.getParameterTypes()) {
                    try {
                        Object proxyInstance = Proxy.newProxyInstance(
                                MetadataLoader.class.getClassLoader(),
                                new Class[]{parameterType},
                                new H2RepositoryFacade(parameterType));
                        repositoryList.add(proxyInstance);
                    } catch (Exception e){
                        logger.error(e.getMessage());
                    }
                }

                obj = ctor.newInstance(repositoryList.toArray());
            }

            Class<IEvent> outputType = (Class<IEvent>) method.getReturnType();

            List<Class<IEvent>> inputTypes = new ArrayList<>();

            for(int i = 0; i < method.getParameters().length; i++){
                inputTypes.add( (Class<IEvent>) method.getParameters()[i].getType());
            }

            String[] inputQueues = null;

            Annotation[] ans = method.getAnnotations();
            DataOperationSignature dataOperation = new DataOperationSignature(obj, method);
            for (Annotation an : ans) {
                logger.info("Mapped = " + an.toString());
                if(an instanceof Inbound){
                    inputQueues = ((Inbound) an).values();
                    dataOperation.inputQueues = inputQueues;
                    for(int i = 0; i < inputQueues.length; i++) {

                        if(config.queueToEventMap.get(inputQueues[i]) == null){
                            config.queueToEventMap.put(inputQueues[i], inputTypes.get(i));
                        } else {
                            throw new MappingException("Error mapping. An input queue cannot be mapped to tow or more event types.");
                        }

                        List<DataOperationSignature> list = config.eventToOperationMap.get(inputQueues[i]);
                        if(list == null){
                            list = new ArrayList<>();
                            list.add(dataOperation);
                            config.eventToOperationMap.put(inputQueues[i], list);
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
                    config.queueToEventMap.put(outputQueue,outputType);
                }
            }

        }

        return config;

    }

}
