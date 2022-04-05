package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

public class EventExample implements IEvent {

    public int id;

    public EventExample(int id) {
        this.id = id;
    }
}