package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.ArrayList;
import java.util.List;

public class EventIdentifier {

    public String alias; // alias for fast identification in topology definition
    public String vms; // virtual microservice
    public boolean terminal; // terminal node

    /** Attributes used only if it is not terminal */
    public String event; // event name only if it is not terminal
    public List<EventIdentifier> children;

    // in case a children is terminal

    public EventIdentifier(String alias, String vms, String event) {
        this.alias = alias;
        this.vms = vms;
        this.event = event;
        this.terminal = false;
    }

    public EventIdentifier(String alias, String vms) {
        this.alias = alias;
        this.vms = vms;
        this.event = null;
        this.terminal = true;
    }

    public void addChildren( EventIdentifier event ){
        if (terminal) return;
        if(children == null) children = new ArrayList<>();
        children.add(event);
    }

}
