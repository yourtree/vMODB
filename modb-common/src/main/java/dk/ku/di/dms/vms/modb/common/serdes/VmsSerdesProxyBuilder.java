package dk.ku.di.dms.vms.modb.common.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A builder of serialization and deserialization capabilities
 * The idea is to abstract in this class the procedures to transform objects,
 * so later we can change without disrupting the client classes, like VmsEventHandler
 * A comparison:
 * <a href="https://www.overops.com/blog/the-ultimate-json-library-json-simple-vs-gson-vs-jackson-vs-json/">Why GSON?</a>
 * "If your environment primarily deals with lots of small JSON requests, such as in a microservice
 * or distributed architecture setup, then GSON is your library of interest. Jackson struggles the most with small files."
 */
public final class VmsSerdesProxyBuilder {

    public static IVmsSerdesProxy build(){
        GsonBuilder builder = new GsonBuilder();
        Gson gson1 = builder.serializeNulls().create();
        return new DefaultVmsSerdes( gson1 );
    }

}
