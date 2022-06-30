package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.modb.common.event.IVmsApplicationEvent;

public class EventExample implements IVmsApplicationEvent {

    public int id;

    public EventExample(int id) {
        this.id = id;
    }
}