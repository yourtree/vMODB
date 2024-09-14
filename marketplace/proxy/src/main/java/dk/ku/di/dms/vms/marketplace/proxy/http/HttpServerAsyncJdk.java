package dk.ku.di.dms.vms.marketplace.proxy.http;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

public final class HttpServerAsyncJdk extends AbstractHttpHandler implements IHttpHandler {

    public HttpServerAsyncJdk(Coordinator coordinator) {
        super(coordinator);
    }

    public void post(String uri, String body) {
        this.submitCustomerCheckout(body);
    }

    public byte[] getAsBytes(String uri) {
        String[] uriSplit = uri.split("/");
        if (uriSplit[1].equals("status")) {
            if(uriSplit[2].equals("committed")) {
                return this.getNumTIDsCommittedBytes();
            }
            return this.getNumTIDsSubmittedBytes();
        }
        return null;
    }

    public void patch(String uri, String body) {
        this.submitUpdatePrice(body);
    }

    public void put(String uri, String body) {
        this.submitUpdateProduct(body);
    }

}
