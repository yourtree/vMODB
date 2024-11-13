package dk.ku.di.dms.vms.tpcc.proxy.infra;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.infra.AbstractHttpHandler;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

/**
 * Necessary?
 */
public final class TPCcHttpHandler extends AbstractHttpHandler implements IHttpHandler {

    public TPCcHttpHandler(Coordinator coordinator) {
        super(coordinator);
    }

    public void post(String uri, String body) {
        TransactionInput.Event eventPayload = new TransactionInput.Event("new_order", body);
        TransactionInput txInput = new TransactionInput("new_order", eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }

}
