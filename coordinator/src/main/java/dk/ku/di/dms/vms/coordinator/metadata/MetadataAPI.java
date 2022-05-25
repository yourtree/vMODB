package dk.ku.di.dms.vms.coordinator.metadata;

import dk.ku.di.dms.vms.coordinator.VmsMetadata;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;

import java.io.RandomAccessFile;
import java.util.Map;

/**
 * Class that provides facilities to manage the metadata in main memory
 */
public class MetadataAPI {

    private RandomAccessFile raf;

    // indexes for fast access in runtime
    private int posVmsMetadata;

    // position of a given transaction in the buffer
    private Map<String,Integer> transactionPositionMap;

    // position of a given vms metadata in the buffer
    private Map<String,Integer> vmsMetadataPositionMap;

    public MetadataAPI(RandomAccessFile raf){



    }

    public boolean updateOffset(float offset){
        return false;
    }

    public boolean registerTransaction(TransactionDAG transaction){
        // update number of transactions and then register the transaction according to the schema
        return false;
    }

    public boolean registerVmsMetadata(VmsMetadata vmsMetadata){
        return false;
    }


}
