package dk.ku.di.dms.vms.modb.common.query.clause;

import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;

public record WhereClauseElement<T>(String column,
                                    ExpressionTypeEnum expression,
                                    T value) {}
