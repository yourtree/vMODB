package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.*;

/**
 * Responsible for assembling the topology of a transaction crossing virtual microservices
 */
public final class TransactionBootstrap {

    private final List<EventIdentifier> inputEvents; // the input events

    // for fast seek
    private final Map<String, EventIdentifier> inputEventToInternalVMSsMap;

    private String name; // transaction name

    // to allow the coordinator to efficiently assign the lastTid to each internal node of a transaction
    private final Set<String> internalNodes;

    private final List<String> terminalNodes;

    public TransactionBootstrap(){
        this.inputEvents = new ArrayList<>();
        this.internalNodes = new HashSet<>();
        this.terminalNodes = new ArrayList<>();
        this.inputEventToInternalVMSsMap = new HashMap<>();
    }

    public TransactionBootstrapBuilder init(String name){
        this.name = name;
        return TransactionBootstrapBuilder.build(this);
    }

    /**
     * Not thread-safe
     */
    public static class TransactionBootstrapBuilder {

        private TransactionBootstrap transactionBootstrap;
        private static final TransactionBootstrapBuilder INSTANCE = new TransactionBootstrapBuilder();

        protected static TransactionBootstrapBuilder build(TransactionBootstrap transactionBootstrap){
            INSTANCE.transactionBootstrap = transactionBootstrap;
            return INSTANCE;
        }

        public TransactionBootstrapBuilder input(String alias, String vms, String event){
            EventIdentifier id = new EventIdentifier( alias, vms, event );
            transactionBootstrap.inputEventToInternalVMSsMap.put( alias, id );
            transactionBootstrap.inputEvents.add( id );
            return this;
        }

        public TransactionBootstrapBuilder internal(String alias, String vms, String event, String... deps){
            if(deps == null) throw new RuntimeException("Cannot have an internal event without a parent event");
            EventIdentifier toAdd = new EventIdentifier( alias, vms, event );
            for(String dep : deps){
                EventIdentifier id = transactionBootstrap.inputEventToInternalVMSsMap.get(dep);
                id.addChildren( toAdd );
            }
            transactionBootstrap.inputEventToInternalVMSsMap.put( alias, toAdd );
            transactionBootstrap.internalNodes.add(vms);
            return this;
        }

        public TransactionBootstrapBuilder terminal(String alias, String vms, String... deps){
            if(deps == null) throw new RuntimeException("Cannot have a terminal event without a parent event");
            EventIdentifier terminal = new EventIdentifier(alias, vms);
            transactionBootstrap.terminalNodes.add(terminal.targetVms);
            for(String dep : deps){
                EventIdentifier id = transactionBootstrap.inputEventToInternalVMSsMap.get(dep);
                // terminal.addDependence( id );
                id.addChildren( terminal );
            }
            return this;
        }

        // finally, build the transaction representation
        public TransactionDAG build(){
            transactionBootstrap.inputEvents.sort(Comparator.comparing(o -> o.name));
            return new TransactionDAG(transactionBootstrap.name, transactionBootstrap.inputEvents, transactionBootstrap.internalNodes, transactionBootstrap.terminalNodes);
        }

    }

}
