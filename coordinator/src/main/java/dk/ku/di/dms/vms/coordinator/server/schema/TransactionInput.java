package dk.ku.di.dms.vms.coordinator.server.schema;

import java.util.Collections;
import java.util.List;

/**
 * This is purely for parsing transaction requests coming from HTTP clients
 */
public class TransactionInput {

    //name of the transaction
    public String name;

    public List<Event> events;

    public TransactionInput(String name, Event event) {
        this.name = name;
        this.events = Collections.singletonList(event);
    }

    public static class Event {
        public String name; // event name
        public String payload; // event payload... I can avoid deserializing the payload for higher performance. but at the end

        public Event(String name, String payload) {
            this.name = name;
            this.payload = payload;
        }
    }

    // for stream operations
    public String getName() {
        return name;
    }

}
