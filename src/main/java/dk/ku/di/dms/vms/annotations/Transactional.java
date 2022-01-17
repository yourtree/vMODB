package dk.ku.di.dms.vms.annotations;

import dk.ku.di.dms.vms.enums.IsolationLevelEnum;
import dk.ku.di.dms.vms.enums.TransactionPropagationEnum;
import dk.ku.di.dms.vms.enums.TransactionTypeEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static dk.ku.di.dms.vms.enums.IsolationLevelEnum.SERIALIZABLE;
import static dk.ku.di.dms.vms.enums.TransactionPropagationEnum.REQUIRES_NEW;
import static dk.ku.di.dms.vms.enums.TransactionTypeEnum.RW;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Transactional {
    TransactionTypeEnum type() default RW;
    IsolationLevelEnum isolation() default SERIALIZABLE;
    TransactionPropagationEnum propagation() default REQUIRES_NEW;
}