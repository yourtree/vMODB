package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.operational.DataOperationSignature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data class to store the mappings between events, queues, and operations
 */

public class ApplicationMetadata {

    public final Map<String, List<DataOperationSignature>> eventToOperationMap = new HashMap<>();
    public final Map<String,Class<IEvent>> queueToEventMap = new HashMap<>();

}
