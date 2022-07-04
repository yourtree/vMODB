package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.sdk.core.annotations.Event;

@Event
public class EventExample {

    public int id;

    public EventExample(int id) {
        this.id = id;
    }
}