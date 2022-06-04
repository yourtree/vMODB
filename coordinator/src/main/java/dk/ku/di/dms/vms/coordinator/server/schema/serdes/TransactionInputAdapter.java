package dk.ku.di.dms.vms.coordinator.server.schema.serdes;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import dk.ku.di.dms.vms.coordinator.server.schema.external.TransactionInput;

import java.io.IOException;

public class TransactionInputAdapter extends TypeAdapter<TransactionInput> {


    @Override
    public void write(JsonWriter out, TransactionInput value) throws IOException {

    }

    @Override
    public TransactionInput read(JsonReader in) throws IOException {
        return null;
    }
}
