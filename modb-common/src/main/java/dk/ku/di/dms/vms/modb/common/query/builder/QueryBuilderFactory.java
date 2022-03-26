package dk.ku.di.dms.vms.modb.common.query.builder;

/**
 * The objective of this class is to provide an easy way to build SQL
 * queries in an object-oriented manner. Inspired by jooq: <a>https://www.jooq.org</a>
 */

public final class QueryBuilderFactory {

    public static SelectStatementBuilder select() {
        return new SelectStatementBuilder();
    }

    public static UpdateStatementBuilder update() {
        return new UpdateStatementBuilder();
    }

}
