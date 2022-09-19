package dk.ku.di.dms.vms.modb.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An anonymous class annotated with Disjoint means
 * such code is data-race free.
 *
 * Operates over disjoint data items. his way the concurrency control
 * does not need to make serializability checks.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE_USE)
public @interface Disjoint {

}