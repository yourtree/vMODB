package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parser.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.database.query.parser.builder.UpdateStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The objective of this class is to provide an easy way to build SQL
 * queries in an object-oriented manner. Inspired by jooq: <a>https://www.jooq.org</a>
 */

public final class QueryBuilderFactory {

     final private static Logger logger = LoggerFactory.getLogger(QueryBuilderFactory.class);


    public static SelectStatementBuilder select() {
        return new SelectStatementBuilder();
    }

    public static UpdateStatementBuilder update() {
        return new UpdateStatementBuilder();
    }

}
