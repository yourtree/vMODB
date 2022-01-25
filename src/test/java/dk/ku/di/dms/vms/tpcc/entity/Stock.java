package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="stock")
@IdClass(Stock.StockId.class)
public class Stock extends AbstractEntity<Stock.StockId> {

    public class StockId implements Serializable {
        public Integer s_i_id;
        public Integer s_w_id;

        public StockId(Integer s_i_id, Integer s_w_id) {
            this.s_i_id = s_i_id;
            this.s_w_id = s_w_id;
        }
    }

    @Id
    public Integer s_i_id;

    @Id
    public Integer s_w_id;

    @Column
    public Integer s_quantity;

    @Column
    public String s_data;

    @Column
    public String s_dist;

    public Stock(){}

}
