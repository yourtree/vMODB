package dk.ku.di.dms.vms.coordinator.transaction;

/**
 * This is purely for parsing transaction requests coming from HTTP clients
 * The name of the event is not always the name of the transaction
 * An internal VMs, for example, is subject to these phenomena
 * (e.g., update shipment transaction and the invoice issued event)
 */
public class TransactionInput {

    //name of the transaction
    public String name;

    public Event event;

    public TransactionInput(String name, Event event) {
        this.name = name;
        this.event = event;
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
