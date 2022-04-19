package dk.ku.di.dms.vms.web_common.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.function.Function;

public class VmsSerdesProxyBuilder {

    public static IVmsSerdesProxy build(final Function<String,Class<? extends IEvent>> clazzResolver){

        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(TransactionalEvent.class, new TransactionalEventAdapter( clazzResolver ));
        builder.setPrettyPrinting();
        Gson gson1 = builder.create();

        return new IVmsSerdesProxy() {

            private final Gson gson = gson1;

            @Override
            public String toJson(Object src) {
                return gson.toJson(src);
            }

            @Override
            @SuppressWarnings("unchecked")
            public TransactionalEvent fromJson(String json) {
                return gson.fromJson(json, TransactionalEvent.class);
            }
        };

    }

}
