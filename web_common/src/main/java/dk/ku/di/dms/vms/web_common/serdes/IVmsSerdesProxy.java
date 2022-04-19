package dk.ku.di.dms.vms.web_common.serdes;

public interface IVmsSerdesProxy {

    String toJson(Object src);
    <T> T fromJson(String json);

}
