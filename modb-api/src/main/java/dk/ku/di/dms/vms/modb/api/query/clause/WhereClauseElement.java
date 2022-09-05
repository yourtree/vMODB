package dk.ku.di.dms.vms.modb.api.query.clause;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;

public record WhereClauseElement<T>(String column,
                                    ExpressionTypeEnum expression,
                                    T value) {}
