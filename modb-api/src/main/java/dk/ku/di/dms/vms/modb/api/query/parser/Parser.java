package dk.ku.di.dms.vms.modb.api.query.parser;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Only parses simple SELECT statements
 */
public class Parser {

    /**
     *
     * @param sql a simple select statement (select <x,y,x,...> from table where <>
     */
    public static SelectStatement parse(String sql) {

        String[] tokens = sql.split(" ");

        assert tokens.length > 0 && tokens[0].equalsIgnoreCase("select");

        int i = 1;

        List<String> projection = new ArrayList<>(2);

        while(i < tokens.length && !tokens[i].equalsIgnoreCase("from")){
            // remove comma from all
            projection.add(tokens[i].replace(',',' ').trim());
            i++;
        }

        i++;
        assert i < tokens.length;
        String table = tokens[i];
        i++;
        assert i < tokens.length && tokens[i].equalsIgnoreCase("where");
        i++;

        List<WhereClauseElement<?>> whereClauseElements = new ArrayList<>(2);

        // get triples of values
        while(i < tokens.length){
            // remove comma from all
            var left = tokens[i];
            i++;
            var exp = getExpressionFromString(tokens[i]);
            if(exp != ExpressionTypeEnum.IS_NULL && exp != ExpressionTypeEnum.IS_NOT_NULL){
                i++;
                var right = tokens[i];
                whereClauseElements.add( new WhereClauseElement<>(left, exp, Optional.empty()) );
            } else {
                whereClauseElements.add( new WhereClauseElement<>(left, exp, null) );
            }

            i++;

        }

        return new SelectStatement(projection, table, whereClauseElements);

    }

    private static ExpressionTypeEnum getExpressionFromString(String exp){

        if (ExpressionTypeEnum.EQUALS.name.equalsIgnoreCase(exp)) {
            return ExpressionTypeEnum.AND;
        }

        // TODO complete
        return ExpressionTypeEnum.EQUALS;

    }

}
