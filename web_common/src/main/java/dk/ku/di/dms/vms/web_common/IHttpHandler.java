package dk.ku.di.dms.vms.web_common;

public interface IHttpHandler {

    default void post(String uri, String body) throws Exception { }

    default String getAsJson(String uri) throws Exception { return null; }

    default byte[] getAsBytes(String uri) { return null; }

    default void patch(String uri, String body) throws Exception { }

    default void put(String uri, String body) throws Exception { }

}