package dk.ku.di.dms.vms.sdk.embed.scheduler;

import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.BatchContext;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * For the embedded scenario, we can have direct access to {@link TransactionFacade}
 */
public class EmbedVmsTransactionScheduler extends VmsTransactionScheduler {

    private final TransactionFacade transactionFacade;

    private final VmsEmbedInternalChannels vmsChannels;

    public EmbedVmsTransactionScheduler(ExecutorService vmsAppLogicTaskPool,
                                        VmsEmbedInternalChannels vmsChannels,
                                        Map<String, VmsTransactionMetadata> eventToTransactionMap,
                                        Map<String, Class<?>> queueToEventMap,
                                        IVmsSerdesProxy serdes,
                                        TransactionFacade transactionFacade) {
        super(vmsAppLogicTaskPool, vmsChannels, eventToTransactionMap, queueToEventMap, serdes);
        this.transactionFacade = transactionFacade;
        this.vmsChannels = vmsChannels;
    }

    @Override
    public void run() {

        logger.info("Scheduler has started.");

        initializeOffset();

        while(isRunning()) {

            checkForNewEvents();

            moveOffsetPointerIfNecessary();

            // process batch before sending tasks to execution
            processBatchCommit();

            processTaskResult();

        }

    }

    private void processBatchCommit() {

        if(!this.vmsChannels.batchCommitRequestQueue().isEmpty()){

            BatchContext currentBatch = this.vmsChannels.batchCommitRequestQueue().remove();

            currentBatch.setStatus(BatchContext.Status.LOGGING);

            // of course, I do not need to stop the scheduler on commit
            // I need to make access to the data versions data race free
            // so new transactions get data versions from the version map or the store
            this.transactionFacade.log();

            currentBatch.setStatus(BatchContext.Status.COMMITTED);

        }

    }

}
