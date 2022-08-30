package dk.ku.di.dms.vms.sdk.core.annotations;

import dk.ku.di.dms.vms.modb.common.interfaces.application.IEntity;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD})
@Retention(RUNTIME)
public @interface VmsForeignKey {

    Class<? extends IEntity<?>> table();

    String column();

}