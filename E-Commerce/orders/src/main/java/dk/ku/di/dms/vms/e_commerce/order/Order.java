package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="order")
public class Order implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    public long id;

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

    // card

//    @Column
//    public String longNum;
//
//    @Column
//    public Date expires;
//
//    @Column
//    public String ccv;

    @Column
    public Date date;

    @Column
    public float total;

    // NEW, PAYMENT_NOT_AUTHORIZED, SHIPMENT_REQUESTED, FINISHED
    public String orderStatus;

    // total time... it took to finish delivery/shipment process
    // items can also have a city...
    // we can have different warehouses

    public Order(Customer customer, float total){

        this.customerId = customer.customerId;
        this.firstName = customer.firstName;
        this.lastName = customer.lastName;
        this.username = customer.username;

//        this.longNum = customer.longNum;
//        this.expires = customer.expires;
//        this.ccv = customer.ccv;

        this.date = new Date();

        this.street = customer.street;
        this.number = customer.number;
        this.country = customer.country;
        this.city = customer.city;
        this.postCode = customer.postCode;

        this.total = total;

        this.orderStatus = "NEW";
    }

}
