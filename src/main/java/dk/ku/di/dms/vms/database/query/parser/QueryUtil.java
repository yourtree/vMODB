package dk.ku.di.dms.vms.database.query.parser;

import dk.ku.di.dms.vms.database.query.parser.enums.StatementTypeEnum;

import java.util.HashSet;
import java.util.Set;

public class QueryUtil {

    private static final Set<String> keywordsMap;

    //private static

    public boolean isKeyword(String value){
        return keywordsMap.contains(value);
    }

    static {
        keywordsMap = new HashSet<>();

        StatementTypeEnum[] statementEnums = StatementTypeEnum.values();
        StatementTypeEnum[] expressionEnums = StatementTypeEnum.values();

        for(int i = 0; i < statementEnums.length; i++){
            keywordsMap.add( statementEnums[i].name() );
        }

        for(int i = 0; i < expressionEnums.length; i++){
            keywordsMap.add( expressionEnums[i].name() );
        }

    }

}
