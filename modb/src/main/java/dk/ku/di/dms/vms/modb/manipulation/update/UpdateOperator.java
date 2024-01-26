package dk.ku.di.dms.vms.modb.manipulation.update;

import dk.ku.di.dms.vms.modb.api.query.statement.UpdateStatement;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

/**
 * Two strategies:
 * (i) as the records are found, they are updated
 * (ii) find all records, iterate over them applying the updates
 * We can go for (i), taking advantage over consistent index
 */
public final class UpdateOperator {

    public static void run(UpdateStatement updateStatement, PrimaryIndex index, FilterContext filterContext, IKey... keys){

        if(index.underlyingIndex().getType() == IndexTypeEnum.UNIQUE){

            for(IKey key : keys){

                Object[] record = index.lookupByKey(key);
                if(record == null) continue;

                Object[] cloned = record.clone();
                if(index.checkConditionVersioned(filterContext, cloned)){

                    for(var setClause : updateStatement.setClause){
                        cloned[ index.underlyingIndex().schema().columnPosition( setClause.column() )] = setClause.value();
                    }

                    index.update( key, cloned );

                }


            }

        }

    }

    public static void run(UpdateStatement updateStatement, PrimaryIndex index, FilterContext filterContext){



    }

}
