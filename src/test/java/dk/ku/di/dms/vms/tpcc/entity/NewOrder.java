package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name="new_orders")
@IdClass(NewOrder.NewOrderId.class)
public class NewOrder extends AbstractEntity<NewOrder.NewOrderId> {

    public class NewOrderId implements Serializable {
        public Integer no_o_id;
        public Integer no_d_id;
        public Integer no_w_id;

        public NewOrderId(Integer no_o_id, Integer no_d_id, Integer no_w_id) {
            this.no_o_id = no_o_id;
            this.no_d_id = no_d_id;
            this.no_w_id = no_w_id;
        }
    }

    @Id
    public Integer no_o_id;

    @Id
    public Integer no_d_id;

    @Id
    public Integer no_w_id;

    public NewOrder(){}

    public NewOrder(Integer no_o_id, Integer no_d_id, Integer no_w_id) {
        this.no_o_id = no_o_id;
        this.no_d_id = no_d_id;
        this.no_w_id = no_w_id;
    }
}
