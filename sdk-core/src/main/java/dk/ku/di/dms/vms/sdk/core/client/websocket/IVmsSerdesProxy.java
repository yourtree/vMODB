package dk.ku.di.dms.vms.sdk.core.client.websocket;

public interface IVmsSerdesProxy {

    String toJson(Object src);
    <T> T fromJson(String json);

}
