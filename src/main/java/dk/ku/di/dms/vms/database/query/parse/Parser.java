package dk.ku.di.dms.vms.database.query.parse;

public class Parser {


    // 1 - check syntax
    // 2 - read string and break into operator nodes
    // 3 - build query tree

    // TODO leave it for later. just rely on a query API
    public Statement parse(String sql){

        String[] tokens = sql.split(" ");
        String curr;
        for(int i = 0; i < tokens.length; i++){
            curr = tokens[i];
            char currFirstChar = curr.charAt(0);
            switch(currFirstChar){
                case 'S':
                case 's': {
                    // SELECT OR UPDATE SET

                    // if select, we need to obtain all columns
                    if(equivalent(curr, StatementEnum.SELECT)){
                        // iterate until we see a keyword
                        // for basic criteria, until we see a FROM
                        int j = i + 1;
                        String next = tokens[j];

                        while(!equivalent(next, StatementEnum.FROM)){

                        }

                    } else {
                        // anything else is wrong
                        // because the update token should
                        // have been advanced the iteration by this point
                    }
                    /*
                        if(equivalent(curr,KeywordEnum.SET)){

                    }
                    */

                    break;
                }
            }

        }

        return null;

    }

    private boolean equivalent(String token, StatementEnum keywordEnum){
        String keywordStr = keywordEnum.name();
        return token.equalsIgnoreCase(keywordStr);
    }

}