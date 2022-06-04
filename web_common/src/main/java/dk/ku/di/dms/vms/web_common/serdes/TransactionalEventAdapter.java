package dk.ku.di.dms.vms.web_common.serdes;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * https://www.tutorialspoint.com/gson/gson_custom_adapters.htm
 */
public class TransactionalEventAdapter extends TypeAdapter<TransactionalEvent> {

    private final Map<String, Class<? extends IApplicationEvent>> queueToEventMap;

    private final Gson gson;
    
    public TransactionalEventAdapter( Map<String, Class<? extends IApplicationEvent>> queueToEventMap ) {
        this.queueToEventMap = queueToEventMap;
        this.gson = new Gson();
    }

    @Override
    public void write(JsonWriter out, TransactionalEvent value) throws IOException {
        out.setHtmlSafe(false);
        out.setLenient(true);
        out.setIndent("");
        out.beginObject();
        out.name("tid");
        out.value(value.tid());
        out.name("queue");
        out.value(value.queue());
        out.name("payload");
        out.jsonValue( new Gson().toJson( value.payload() ) );
        out.endObject();
    }

    @Override
    public TransactionalEvent read(JsonReader in) throws IOException {

        int tid = 0;
        String queue = null;
        IApplicationEvent event = null;

        in.beginObject();
        in.peek();

        String attributeName = in.nextName();
        in.peek();

        if ("tid".equals(attributeName)) {
            in.peek();
            tid = in.nextInt();
        }

        in.peek();
        attributeName = in.nextName();

        if("queue".equals(attributeName)) {
            in.peek();
            queue = in.nextString();
        }

        in.peek();
        attributeName = in.nextName();

        if("payload".equals(attributeName)) {
            in.peek();
            Type typeOfT = queueToEventMap.get( queue );
            if(typeOfT != null) {
                event = gson.fromJson(in, typeOfT);
            }
        }

        in.endObject();

        return new TransactionalEvent( tid, queue, event );

    }

}