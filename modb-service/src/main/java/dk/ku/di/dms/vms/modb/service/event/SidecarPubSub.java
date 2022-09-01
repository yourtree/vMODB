package dk.ku.di.dms.vms.modb.service.event;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;

import java.util.LinkedList;
import java.util.Queue;

public class SidecarPubSub {
    
    public final Queue<Object> inputQueue;

    public final Queue<Object> outputQueue;

    public final Queue<DataRequestEvent> requestQueue;

    public final  Queue<DataResponseEvent> responseQueue;

    public static SidecarPubSub newInstance(){
        return new SidecarPubSub();
    }

    private SidecarPubSub(){
        this.inputQueue = new LinkedList<>();
        this.outputQueue = new LinkedList<>();
        this.requestQueue = new LinkedList<>();
        this.responseQueue = new LinkedList<>();
    }

}