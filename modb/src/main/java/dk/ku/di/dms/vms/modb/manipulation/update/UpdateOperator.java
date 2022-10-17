package dk.ku.di.dms.vms.modb.manipulation.update;

import dk.ku.di.dms.vms.modb.api.query.statement.UpdateStatement;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.ConsistentIndex;

/**
 * Two strategies:
 * (i) as the records are found, they are updated
 * (ii) find all records, iterate over them applying the updates
 * We can go for (i), taking advantage over consistent index
 */
public final class UpdateOperator {

    public static void run(UpdateStatement updateStatement, ConsistentIndex index, FilterContext filterContext, IKey... keys){

        if(index.getType() == IndexTypeEnum.UNIQUE){

            for(IKey key : keys){

                Object[] record = index.lookupByKey(key);
                if(record == null) continue;

                Object[] cloned = record.clone();
                if(index.checkConditionVersioned(filterContext, cloned)){

                    for(var setClause : updateStatement.setClause){
                        cloned[ index.schema().getColumnPosition( setClause.column() )] = setClause.value();
                    }

                    index.update( key, cloned );

                }


            }

        }

    }

    public static void run(UpdateStatement updateStatement, ConsistentIndex index, FilterContext filterContext){



    }

}
