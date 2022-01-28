package dk.ku.di.dms.vms.database.api.modb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The objective of this class is to provide an easy way to build SQL
 * queries in an object-oriented manner. Inspired by jooq: <a>https://www.jooq.org</a>
 */

public final class QueryBuilderFactory {

    final private static Logger logger = LoggerFactory.getLogger(QueryBuilder.class);

    public static IQueryBuilder init() {
//        try{
//            Class cls = Class.forName(QueryBuilder.class.getCanonicalName());
//            return (QueryBuilder) cls.newInstance();
//        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e){
//            logger.error(e.getMessage());
//            return null;
//        }
        return new QueryBuilder();
    }

}
