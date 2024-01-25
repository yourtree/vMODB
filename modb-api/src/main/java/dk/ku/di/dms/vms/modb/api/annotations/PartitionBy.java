package dk.ku.di.dms.vms.modb.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * <a href="https://stackoverflow.com/questions/44291122/how-do-i-pass-a-method-to-an-annotation-using-java-8">...</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PartitionBy {

    Class<?> clazz();
    String method();

}
