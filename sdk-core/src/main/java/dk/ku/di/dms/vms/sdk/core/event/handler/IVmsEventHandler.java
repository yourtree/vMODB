package dk.ku.di.dms.vms.sdk.core.event.handler;


import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.net.InetSocketAddress;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 *
 * Interface that makes clear what the VMS event handler must do
 * The class implementing this interface is oblivious of how the
 * messages are unpacked, sent, and received over the network
 *
 * It only deals with logic involving the VMS transaction execution
 *
 * MAYBE: Make the handler async like this project:
 * https://github.com/ebarlas/microhttp
 *
 * Inspired by
 * {@link java.net.http.WebSocket}
 *
 */
public interface IVmsEventHandler {

    /**
     * Can come from both the leader and (producer) VMSs
     * @param transactionEventPayload
     */
    void onTransactionInputEvent(TransactionEvent.Payload transactionEventPayload);

    void onBatchCommitRequest(BatchCommitRequest.Payload batchCommitReq);

    void onBatchAbortRequest(BatchAbortRequest.Payload batchAbortReq);

    void onTransactionAbort(TransactionAbort.Payload transactionAbortReq);

    /**
     * Receives a presentation payload from the leader
     * @param payload presentation
     */
    void onLeaderConnection(Presentation.PayloadFromServer payload);

    /**
     * Receives a presentation payload from a (producer) VMS
     * @param vms vms
     */
    void onVmsConnection(VmsIdentifier vms);

}