package dk.ku.di.dms.vms.modb.common.query.clause;

import dk.ku.di.dms.vms.modb.common.query.enums.GroupByOperationEnum;

/**
 * The actual operation that should be carried out in this column
 */
public record GroupBySelectElement(String column,
                                   GroupByOperationEnum operation) {}