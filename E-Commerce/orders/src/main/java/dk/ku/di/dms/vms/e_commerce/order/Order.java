package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.entity.Address;
import dk.ku.di.dms.vms.e_commerce.common.entity.Card;
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

    // card

    @Column
    public String longNum;

    @Column
    public Date expires;

    @Column
    public String ccv;

    @Column
    public Date date;

    @Column
    private float total;

    public Order(Customer customer, Card card, Address address, float total){

        this.firstName = customer.firstName;
        this.lastName = customer.lastName;
        this.username = customer.username;

        this.longNum = card.longNum;
        this.expires = card.expires;
        this.ccv = card.ccv;

        this.date = new Date();

        this.street = address.street;
        this.number = address.number;
        this.country = address.country;
        this.city = address.city;
        this.postCode = address.postCode;

        this.total = total;
    }

}
