package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="stock")
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
    public int s_i_id;

    @Id
    public int s_w_id;

    @Column
    public int s_quantity;

    @Column
    public String s_data;

    @Column
    public String s_dist;

    public Stock(){}

    public Stock(int s_i_id, int s_w_id, int s_quantity, String s_data, String s_dist) {
        this.s_i_id = s_i_id;
        this.s_w_id = s_w_id;
        this.s_quantity = s_quantity;
        this.s_data = s_data;
        this.s_dist = s_dist;
    }
}
