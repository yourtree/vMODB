package dk.ku.di.dms.vms.coordinator.server.schema;

import java.util.ArrayList;

/**
 * This is purely for parsing transaction requests coming from HTTP clients
 */
public class TransactionInput {

    //name of the transaction
    public String name;

    public ArrayList<Event> events;

    public static class Event {
        public String name; // event name
        public String payload; // event payload... I can avoid deserializing the payload for higher performance. but at the end
    }

    // for stream operations
    public String getName() {
        return name;
    }

}
