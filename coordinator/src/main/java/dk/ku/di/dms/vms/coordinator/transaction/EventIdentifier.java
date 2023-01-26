package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.ArrayList;
import java.util.List;

public class EventIdentifier {

    public String alias; // alias for fast identification in topology definition

    public String targetVms; // virtual microservice

    public boolean terminal; // terminal node

    /** Attributes used only if it is not terminal */
    public String name; // event name only if it is not terminal

    public List<EventIdentifier> children;

    // in case a children is terminal

    public EventIdentifier(String alias, String targetVms, String name) {
        this.alias = alias;
        this.targetVms = targetVms;
        this.name = name;
        this.terminal = false;
    }

    public EventIdentifier(String alias, String targetVms) {
        this.alias = alias;
        this.targetVms = targetVms;
        this.name = null;
        this.terminal = true;
    }

    public void addChildren(EventIdentifier event){
        if (this.terminal) return;
        if(this.children == null) this.children = new ArrayList<>();
        this.children.add(event);
    }

    // for the stream
    public String getName(){
        return this.name;
    }

}
