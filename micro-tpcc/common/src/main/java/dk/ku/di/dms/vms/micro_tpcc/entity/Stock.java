package dk.ku.di.dms.vms.micro_tpcc.entity;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;

@Entity
@VmsTable(name="stock",
        indexes = {@VmsIndex(name = "fkey_stock_2", columnList = "s_i_id")
})
@IdClass(Stock.StockId.class)
public class Stock implements IEntity<Stock.StockId> {

    public static class StockId implements Serializable {
        public Integer s_i_id;
        public Integer s_w_id;

        public StockId(Integer s_i_id, Integer s_w_id) {
            this.s_i_id = s_i_id;
            this.s_w_id = s_w_id;
        }
    }

    @Id
    @VmsForeignKey(table=Item.class,column = "i_id")
    public int s_i_id;

    @Id
    @VmsForeignKey(table=Warehouse.class,column = "w_id")
    public int s_w_id;

    @Column
    @PositiveOrZero // just for testing...
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
