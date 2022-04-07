package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

/**
 * Make the handler async like this project:
 * https://github.com/ebarlas/microhttp
 */
public interface IVmsEventHandler {

    void handle(TransactionalEvent event);

    void expel(TransactionalEvent event);

}