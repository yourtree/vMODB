package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class OutEventExample2 {

    public final int id;
    public OutEventExample2(int id) {
        this.id=id;
    }
}
