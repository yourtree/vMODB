package dk.ku.di.dms.vms.modb.common.query.builder;

import dk.ku.di.dms.vms.modb.common.query.clause.SetClauseElement;
import dk.ku.di.dms.vms.modb.common.query.statement.UpdateStatement;

public class UpdateStatementBuilder extends AbstractStatementBuilder  {

    private final UpdateStatement statement;

    public UpdateStatementBuilder() {
        this.statement = new UpdateStatement();
    }

    public SetClause update(String param) {
        this.statement.table = param;
        return new SetClause( this.statement );
    }

    public class SetClause {

        private final UpdateStatement statement;

        protected SetClause(UpdateStatement selectStatement){
            this.statement = selectStatement;
        }

        public JoinWhereClauseBridge<UpdateStatement> set(String param, Object value) {
            SetClauseElement setClauseElement = new SetClauseElement( param, value );
            this.statement.setClause.add(setClauseElement);
            return new JoinWhereClauseBridge<>(statement); // return where or join
        }

    }

}
