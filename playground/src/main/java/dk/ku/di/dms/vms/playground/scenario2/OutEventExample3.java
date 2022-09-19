package dk.ku.di.dms.vms.playground.scenario2;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class OutEventExample3 {

    public final int id;
    public OutEventExample3(int id) {
        this.id=id;
    }
}
