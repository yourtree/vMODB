package dk.ku.di.dms.vms.infra;

// TODO make it private and create an interface (IQueryBuilder) with these methods
public final class QueryBuilder {

    private StringBuilder query = new StringBuilder();

    // TODO throw exception if two consecutive invalid declarations are used
    // e.g., FROM and FROM, SELECT and SELECT
    // private Op lastOp;

//        private QueryBuilder() {
//            this.query = new StringBuilder();
//        }

    public QueryBuilder select(String param) {
        query.append("SELECT ").append(param);
        return this;
    }

    public QueryBuilder from(String param) {
        query.append(" FROM ").append(param);
        return this;
    }

    public QueryBuilder where(String param, Object value) {
        query.append(" WHERE ").append(param).append(value);
        return this;
    }

    public QueryBuilder and(String param, Object value) {
        query.append(" AND ").append(param).append(value);
        return this;
    }

    public QueryBuilder join(String param, Object value) {
        query.append(" JOIN ").append(param).append(" ON ").append(value);
        return this;
    }

    public QueryBuilder update(String param) {
        query.append("UPDATE ").append(param);
        return this;
    }

    public QueryBuilder set(String param, Object value) {
        query.append(" SET ").append(param).append(value);
        return this;
    }

    public String build() {
        final String aux = query.toString();
        this.query = new StringBuilder();
        // this.query.deleteCharAt()
        return aux;
    }
}