package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Entity;
import javax.persistence.IdClass;
import java.io.Serializable;

@Entity
@VmsTable(name="stock_items")
@IdClass(Stock.StockId.class)
public class Stock implements IEntity<Stock.StockId> {

    public static class StockId implements Serializable {
        public int seller_id;
        public int product_id;

        public StockId(){}

        public StockId(int seller_id, int product_id) {
            this.seller_id = seller_id;
            this.product_id = product_id;
        }
    }

    public int seller_id;

    public int product_id;
   
    public int qty_available;
  
    public int qty_reserved;
   
    public int order_count;
   
    public int ytd;
    
    public String data;
 
    public String version;

    public Stock() {}



}
