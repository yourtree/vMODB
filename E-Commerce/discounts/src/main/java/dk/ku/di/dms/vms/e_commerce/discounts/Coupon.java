package dk.ku.di.dms.vms.e_commerce.discounts;

import javax.persistence.Column;
import javax.validation.constraints.Positive;

public class Coupon {

    @Column
    public long userId;

    @Column
    @Positive
    public float discount;



}
