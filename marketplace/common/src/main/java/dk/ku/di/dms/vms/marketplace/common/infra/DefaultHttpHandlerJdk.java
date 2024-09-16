package dk.ku.di.dms.vms.marketplace.common.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

public final class DefaultHttpHandlerJdk implements IHttpHandler {

    private final ITransactionManager transactionManager;

    public DefaultHttpHandlerJdk(ITransactionManager transactionManager){
        this.transactionManager = transactionManager;
    }

    @Override
    public void patch(String uri, String body) {
        this.transactionManager.reset();
    }

}
