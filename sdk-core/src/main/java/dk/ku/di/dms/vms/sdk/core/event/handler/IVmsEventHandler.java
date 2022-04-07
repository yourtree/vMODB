package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.function.Consumer;

/**
 * Make the handler async like this project:
 * https://github.com/ebarlas/microhttp
 */

public interface IVmsEventHandler extends Consumer<TransactionalEvent> {}