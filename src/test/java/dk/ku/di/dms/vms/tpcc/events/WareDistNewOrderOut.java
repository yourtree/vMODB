package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;

public class WareDistNewOrderOut extends TransactionalEvent {

    public Float w_tax;
    public Float d_tax;
    public Integer d_next_o_id;
    public Integer d_w_id;
    public Integer d_id;

    public WareDistNewOrderOut(Float w_tax, Float d_tax, Integer d_next_o_id, Integer d_w_id, Integer d_id) {
        this.w_tax = w_tax;
        this.d_tax = d_tax;
        this.d_next_o_id = d_next_o_id;
        this.d_w_id = d_w_id;
        this.d_id = d_id;
    }
}
