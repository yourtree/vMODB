package dk.ku.di.dms.vms.sdk.core.event.pubsub;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.concurrent.BlockingQueue;

public interface IVmsInternalPubSubService extends IPubSubService<Integer, TransactionalEvent> {

    BlockingQueue<TransactionalEvent> inputQueue();
    BlockingQueue<TransactionalEvent> outputQueue();

}
