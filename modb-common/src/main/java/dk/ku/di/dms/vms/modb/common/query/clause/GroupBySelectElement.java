package dk.ku.di.dms.vms.modb.common.query.clause;

import dk.ku.di.dms.vms.modb.common.query.enums.GroupByOperationEnum;

public record GroupBySelectElement(String column,
                                   GroupByOperationEnum operation) {}