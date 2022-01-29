package dk.ku.di.dms.vms.database.query.planner.node;

import dk.ku.di.dms.vms.database.query.analyzer.clause.WhereClause;
import dk.ku.di.dms.vms.database.store.Table;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.lang.reflect.Field;

public class Filter<T extends Table<?,?>> {

    public WhereClause<?> condition;

    public boolean apply(AbstractEntity<?> entity) throws NoSuchFieldException {

        // TODO how can I access a column value without reflection? i would need to
        //  store the fields in the table definition somehow to decrease the cost of accessing the fled dynamically
        Field field = entity.getClass().getField("");
        // field.get()
        // entity
        return false;
    }

}
