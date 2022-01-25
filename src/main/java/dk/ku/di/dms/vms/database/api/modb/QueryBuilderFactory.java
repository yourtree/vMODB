package dk.ku.di.dms.vms.database.api.modb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The objective of this class is to provide an easy way to build SQL
 * queries in an object-oriented manner. Heavily inspired by jooq
 * https://www.jooq.org/
 */

public class QueryBuilderFactory {

    final private static Logger logger = LoggerFactory.getLogger(QueryBuilder.class);

    public static QueryBuilder init() {
        try{
            Class cls = Class.forName(QueryBuilder.class.getCanonicalName());
            return (QueryBuilder) cls.newInstance();
        } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e){
            logger.error(e.getMessage());
            return null;
        }
    }

//    private static void simpleAppend(String param){
//        query.append(param);
//    }

//    private enum Op {
//        SELECT("SELECT "),UPDATE("UPDATE "),SET("SET "),
//        FROM("FROM "),
//        WHERE("WHERE "),
//        AND("AND "),
//        OR("OR ")
//
//        ;
//
//        public final String op;
//
//        private Op(String op) {
//            this.op = op;
//        }
//
//    }


}
