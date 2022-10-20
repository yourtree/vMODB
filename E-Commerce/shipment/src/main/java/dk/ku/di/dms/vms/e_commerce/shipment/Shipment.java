package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.e_commerce.common.entity.Address;
import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.ExternalVmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="shipment")
public class Shipment implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    public long id;

    @Column
    @ExternalVmsForeignKey(vms="order",column = "id")
    public long orderId;

    // customer

    @Column
    public long customer_id;

    @Column
    public String firstName;

    @Column
    public String lastName;

    @Column
    public String username;

    // address
    @Column
    public String street;

    @Column
    public String number;

    @Column
    public String country;

    @Column
    public String city;

    @Column
    public String postCode;

    public Date date;

    public Shipment(Long orderId, Customer customer, Address address){
        this.customer_id = customer.id;
        this.firstName = customer.firstName;
        this.lastName = customer.lastName;
        this.username = customer.username;

        this.date = new Date();

        this.street = address.street;
        this.number = address.number;
        this.country = address.country;
        this.city = address.city;
        this.postCode = address.postCode;
    }

}
