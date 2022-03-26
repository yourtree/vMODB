package dk.ku.di.dms.vms.sdk.core.annotations;

import dk.ku.di.dms.vms.sdk.core.enums.IsolationLevelEnum;
import dk.ku.di.dms.vms.sdk.core.enums.TransactionPropagationEnum;
import dk.ku.di.dms.vms.sdk.core.enums.TransactionTypeEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Transactional {
    TransactionTypeEnum type() default TransactionTypeEnum.RW;
    IsolationLevelEnum isolation() default IsolationLevelEnum.SERIALIZABLE;
    TransactionPropagationEnum propagation() default TransactionPropagationEnum.REQUIRES_NEW;
}