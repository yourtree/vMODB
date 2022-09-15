package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class OutEventExample {

    public final int id;
    public OutEventExample(int id) {
        this.id=id;
    }
}
