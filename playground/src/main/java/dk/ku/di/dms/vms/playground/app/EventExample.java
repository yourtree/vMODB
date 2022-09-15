package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class EventExample {

    public int id;

    public EventExample(int id) {
        this.id = id;
    }
}