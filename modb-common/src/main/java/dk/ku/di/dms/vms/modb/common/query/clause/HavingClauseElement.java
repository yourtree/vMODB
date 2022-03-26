package dk.ku.di.dms.vms.modb.common.query.clause;

import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;

public record HavingClauseElement<T extends Number>(ExpressionTypeEnum expression,
                                                    T value) {}