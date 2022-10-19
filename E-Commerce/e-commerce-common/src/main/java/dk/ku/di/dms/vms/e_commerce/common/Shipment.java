package dk.ku.di.dms.vms.e_commerce.common;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.*;

@Entity
@VmsTable(name="shipment")
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public String id;

    @Column
    public String name;

}
