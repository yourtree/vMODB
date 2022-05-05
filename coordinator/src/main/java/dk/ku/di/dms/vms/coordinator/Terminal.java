package dk.ku.di.dms.vms.coordinator;

import java.util.ArrayList;
import java.util.List;

/**
 * It represents a terminal node in the topology
 */
public class Terminal {

    public String vms;
    public List<EventIdentifier> dependencies;

    public Terminal(String vms) {
        this.vms = vms;
        this.dependencies = new ArrayList<>();
    }

    public void addDependence(EventIdentifier event){
        this.dependencies.add(event);
    }

}
