package dk.ku.di.dms.vms.infra;

import javax.persistence.MappedSuperclass;
import java.io.Serializable;

@MappedSuperclass
public abstract class AbstractEntity<PK extends Serializable> {

    // protected PK primaryKey;

    // default
    protected AbstractEntity(){}

//    public abstract int hashCode();

    //    public AbstractPersistentEntity(PK primaryKey){
//        this.primaryKey = primaryKey;
//    }

    // public abstract PK primaryKey();

}
