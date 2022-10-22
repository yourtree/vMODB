package dk.ku.di.dms.vms.e_commerce.common.entity;

import java.util.Date;

/**
 * Entity that aggregates the information necessary to check out an order
 */
public class Customer {

    public long customerId;

    public String firstName;

    public String lastName;

    public String username;

    public String street;

    public String number;

    public String country;

    public String city;

    public String postCode;

    public String longNum;

    public Date expires;

    public String ccv;

    public Customer() {
    }
}