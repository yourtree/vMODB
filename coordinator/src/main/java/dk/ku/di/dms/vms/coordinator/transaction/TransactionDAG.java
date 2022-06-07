package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransactionDAG {

    public String name; // transaction name
    public Map<String,EventIdentifier> topology;

    public TransactionDAG(String name, List<EventIdentifier> topology) {
        this.name = name;
        this.topology = topology.stream().collect( Collectors.toMap( EventIdentifier::getName, Function.identity() ) );
    }

}