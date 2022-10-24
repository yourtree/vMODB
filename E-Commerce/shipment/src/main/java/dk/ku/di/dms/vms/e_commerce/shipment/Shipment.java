package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.time.LocalTime;
import java.util.Date;

@Entity
@VmsTable(name="shipment")
public class Shipment implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @Column
    public long orderId;

    // customer

    @Column
    public long customerId;

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

    public LocalTime date;

    public Shipment(Long orderId, Customer customer, LocalTime date){

        this.orderId = orderId;

        this.customerId = customer.customerId;
        this.firstName = customer.firstName;
        this.lastName = customer.lastName;
        this.username = customer.username;

        this.date = date;

        this.street = customer.street;
        this.number = customer.number;
        this.country = customer.country;
        this.city = customer.city;
        this.postCode = customer.postCode;
    }

}
