package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransactionDAG {

    public final String name; // transaction name, e.g., new-order
    public final Map<String, EventIdentifier> topology;
    public final List<String> terminals;

    public TransactionDAG(String name, List<EventIdentifier> topology, List<String> terminals) {
        this.name = name;
        this.topology = topology.stream().collect( Collectors.toMap( EventIdentifier::getName, Function.identity() ) );
        this.terminals = terminals;
    }

}