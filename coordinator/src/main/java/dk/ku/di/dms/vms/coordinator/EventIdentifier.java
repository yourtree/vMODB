package dk.ku.di.dms.vms.coordinator;

import java.util.ArrayList;
import java.util.List;

class EventIdentifier{

    public String alias; // alias for fast identification in topology definition
    public String event; // event name
    public String vms; // virtual microservice
    public List<EventIdentifier> children;

    public EventIdentifier(String alias, String vms, String event) {
        this.alias = alias;
        this.vms = vms;
        this.event = event;
    }

    public void addChildren( EventIdentifier event ){
        if(children == null) children = new ArrayList<>();
        children.add(event);
    }

}
