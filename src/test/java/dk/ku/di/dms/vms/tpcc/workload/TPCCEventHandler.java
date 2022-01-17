package dk.ku.di.dms.vms.tpcc.workload;

import dk.ku.di.dms.vms.event.EventRepository;
import dk.ku.di.dms.vms.event.IEventHandler;

public class TPCCEventHandler implements IEventHandler {

    private final EventRepository eventRepository;

    public TPCCEventHandler(EventRepository eventRepository){
        this.eventRepository = eventRepository;
    }

    @Override
    public void run() {

        while(true){

            // produce an event

            // sleep a bit afterwards


        }

    }

}
