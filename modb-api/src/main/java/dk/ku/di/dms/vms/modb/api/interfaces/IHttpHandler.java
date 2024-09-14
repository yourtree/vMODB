package dk.ku.di.dms.vms.modb.api.interfaces;

public interface IHttpHandler {

    default void post(String uri, String body) throws Exception { }

    default String get(String uri) throws Exception { return null; }

    default void patch(String uri, String body) throws Exception { }

    @SuppressWarnings("rawtypes")
    default IRepository repository(String tableName) throws Exception { return null; }

}