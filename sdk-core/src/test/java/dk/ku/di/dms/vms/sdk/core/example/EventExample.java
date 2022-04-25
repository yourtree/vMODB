package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;

public class EventExample implements IApplicationEvent {

    public int id;

    public EventExample(int id) {
        this.id = id;
    }
}