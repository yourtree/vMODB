package dk.ku.di.dms.vms.marketplace.proxy.http;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;

public abstract class AbstractHttpHandler {

    protected final Coordinator coordinator;

    public AbstractHttpHandler(Coordinator coordinator){
        this.coordinator = coordinator;
    }

    protected byte[] getNumTIDsCommittedBytes() {
        long lng = this.coordinator.getNumTIDsCommitted();
        System.out.println("Number of TIDs committed: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    protected byte[] getNumTIDsSubmittedBytes() {
        long lng = this.coordinator.getNumTIDsSubmitted();
        System.out.println("Number of TIDs submitted: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    protected void submitCustomerCheckout(String payload) {
        TransactionInput.Event eventPayload = new TransactionInput.Event(CUSTOMER_CHECKOUT, payload);
        TransactionInput txInput = new TransactionInput(CUSTOMER_CHECKOUT, eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }

    protected void submitUpdatePrice(String payload) {
        TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);
        TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }

    protected void submitUpdateProduct(String payload) {
        TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRODUCT, payload);
        TransactionInput txInput = new TransactionInput(UPDATE_PRODUCT, eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }

    protected void submitUpdateDelivery(String payload) {
        TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_DELIVERY, payload);
        TransactionInput txInput = new TransactionInput(UPDATE_DELIVERY, eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }
}
