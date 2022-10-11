package dk.ku.di.dms.vms.modb.service.handler;

import dk.ku.di.dms.vms.modb.api.query.statement.StatementType;
import dk.ku.di.dms.vms.modb.definition.Table;

import java.util.concurrent.Future;

public class IModbHandler {

    Future<?> onDataRequest(Table table, StatementType statementType, Object[] values){
        return null;
    }

}
