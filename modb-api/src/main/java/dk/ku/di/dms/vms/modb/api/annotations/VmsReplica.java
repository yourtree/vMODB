package dk.ku.di.dms.vms.modb.api.annotations;

import dk.ku.di.dms.vms.modb.api.enums.ReplicationStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE})
@Retention(RUNTIME)
public @interface VmsReplica {

    String virtualMicroservice();

    String table();

    String[] columns();

    ReplicationStrategy strategy() default ReplicationStrategy.STRONG;

}