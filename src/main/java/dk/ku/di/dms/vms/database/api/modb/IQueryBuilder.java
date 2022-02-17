package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;

public interface IQueryBuilder {

    public IQueryBuilder select(String param) throws BuilderException;

    public IQueryBuilder from(String param);

    public IQueryBuilder where(final String param, final ExpressionTypeEnum expr, final Object value);

    public IQueryBuilder where(final String param1, final ExpressionTypeEnum expr, final String param2);

    public IQueryBuilder and(String param, final ExpressionTypeEnum expr, final Object value);

    public IQueryBuilder join(String param);

    public IQueryBuilder on(String param1, ExpressionTypeEnum expression, String param2) throws BuilderException;

    public IQueryBuilder or(String param, final ExpressionTypeEnum expr, final Object value);

    public IQueryBuilder update(String param) throws BuilderException;

    public IQueryBuilder set(String param, Object value);

    public IStatement build();

}
