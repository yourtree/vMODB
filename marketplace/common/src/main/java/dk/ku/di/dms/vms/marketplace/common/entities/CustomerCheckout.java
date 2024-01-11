package dk.ku.di.dms.vms.marketplace.common.entities;

public class CustomerCheckout {

    public int CustomerId;

    public String FirstName;

    public String LastName;

    public String Street;

    public String Complement;

    public String City;

    public String State;

    public String ZipCode;

    public String PaymentType;

    public String CardNumber;

    public String CardHolderName;

    public String CardExpiration;

    public String CardSecurityNumber;

    public String CardBrand;

    // if no credit card; must be 1
    public int Installments;

    public int instanceId;

    public CustomerCheckout(){}

}
