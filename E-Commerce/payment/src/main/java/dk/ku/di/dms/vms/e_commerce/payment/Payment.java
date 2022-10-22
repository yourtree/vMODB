package dk.ku.di.dms.vms.e_commerce.payment;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="payment")
public class Payment implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
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

    // card

    @Column
    public String longNum;

    @Column
    public Date expires;

    @Column
    public String ccv;

    @Column
    public Date date;

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

    @Column
    public boolean authorized;

    public Payment(){}

    public Payment(Customer customer){
        this.customerId = customer.customerId;
        this.firstName = customer.firstName;
        this.lastName = customer.lastName;
        this.username = customer.username;

        this.longNum = customer.longNum;
        this.expires = customer.expires;
        this.ccv = customer.ccv;

        this.date = new Date();

        this.street = customer.street;
        this.number = customer.number;
        this.country = customer.country;
        this.city = customer.city;
        this.postCode = customer.postCode;
    }

}
