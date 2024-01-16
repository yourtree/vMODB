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

    public CustomerCheckout(int customerId, String firstName, String lastName, String street, String complement,
                            String city, String state, String zipCode, String paymentType, String cardNumber,
                            String cardHolderName, String cardExpiration, String cardSecurityNumber, String cardBrand,
                            int installments, int instanceId) {
        CustomerId = customerId;
        FirstName = firstName;
        LastName = lastName;
        Street = street;
        Complement = complement;
        City = city;
        State = state;
        ZipCode = zipCode;
        PaymentType = paymentType;
        CardNumber = cardNumber;
        CardHolderName = cardHolderName;
        CardExpiration = cardExpiration;
        CardSecurityNumber = cardSecurityNumber;
        CardBrand = cardBrand;
        Installments = installments;
        this.instanceId = instanceId;
    }

}
