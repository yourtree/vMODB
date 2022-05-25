package dk.ku.di.dms.vms.coordinator.transaction;

import java.util.List;

public class TransactionDAG {

    public String name; // transaction name
    public List<EventIdentifier> topology;

    public TransactionDAG(String name, List<EventIdentifier> topology) {
        this.name = name;
        this.topology = topology;
    }

}