package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;

public interface IQueryBuilder {

    IQueryBuilder select(String param) throws BuilderException;

    IQueryBuilder from(String param);

    IQueryBuilder where(final String param, final ExpressionTypeEnum expr, final Object value);

    IQueryBuilder where(final String param1, final ExpressionTypeEnum expr, final String param2);

    IQueryBuilder and(String param, final ExpressionTypeEnum expr, final Object value);

    IQueryBuilder join(String table, String column);

    IQueryBuilder on(ExpressionTypeEnum expression, String tableColumnParam) throws BuilderException;

    IQueryBuilder or(String param, final ExpressionTypeEnum expr, final Object value);

    IQueryBuilder update(String param) throws BuilderException;

    IQueryBuilder set(String param, Object value);

    IStatement build();

}
