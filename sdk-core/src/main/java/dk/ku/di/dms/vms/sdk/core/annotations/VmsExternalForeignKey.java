package dk.ku.di.dms.vms.sdk.core.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD})
@Retention(RUNTIME)
public @interface VmsExternalForeignKey {

    String vmsName();

    String table();

    String column();

}