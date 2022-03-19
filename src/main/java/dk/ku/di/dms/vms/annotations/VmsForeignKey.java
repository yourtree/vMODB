package dk.ku.di.dms.vms.annotations;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD})
@Retention(RUNTIME)
public @interface VmsForeignKey {

    Class<? extends AbstractEntity<?>> table();

    String column();

}