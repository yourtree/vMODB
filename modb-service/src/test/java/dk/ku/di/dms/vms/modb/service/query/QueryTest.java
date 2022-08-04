package dk.ku.di.dms.vms.modb.service.query;

import dk.ku.di.dms.vms.modb.catalog.Catalog;
import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.query.planner.operator.projection.TypedProjector;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.schema.ColumnReference;
import dk.ku.di.dms.vms.modb.schema.Row;
import dk.ku.di.dms.vms.modb.schema.Schema;
import dk.ku.di.dms.vms.modb.schema.key.SimpleKey;
import dk.ku.di.dms.vms.modb.service.CustomerInfoDTO;
import dk.ku.di.dms.vms.modb.table.HashIndexedTable;
import dk.ku.di.dms.vms.modb.table.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class QueryTest {

    private static Catalog catalog;

    public static Catalog getDefaultCatalog(){

        // item
        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        DataType[] itemDataTypes = { DataType.INT, DataType.FLOAT, DataType.CHAR, DataType.CHAR  };
        Schema itemSchema = new Schema(itemColumns, itemDataTypes, new int[]{0}, null );
        Table itemTable = new HashIndexedTable("item", itemSchema);

        // customer
        String[] customerColumns = { "c_id", "c_d_id", "c_w_id", "c_discount", "c_last", "c_credit", "c_balance", "c_ytd_payment" };
        DataType[] customerDataTypes = { DataType.LONG, DataType.INT, DataType.INT,
                DataType.FLOAT, DataType.CHAR, DataType.CHAR, DataType.FLOAT, DataType.FLOAT };
        Schema customerSchema = new Schema(customerColumns, customerDataTypes, new int[]{0,1,2}, null );
        Table customerTable = new HashIndexedTable("customer", customerSchema);

        Catalog catalog = new Catalog();

        catalog.insertTables(itemTable,customerTable);

        return catalog;

    }

    @BeforeClass
    public static void buildCatalog(){

        catalog = getDefaultCatalog();

        Table table = catalog.getTable("item");

        SimpleKey key = new SimpleKey(3);
        Row row = new Row(3,2L,"HAHA","HEHE");

        SimpleKey key1 = new SimpleKey(4);
        Row row1 = new Row(4,3L,"HAHAdedede","HEHEdeded");

        table.getPrimaryKeyIndex().update(key, row);
        table.getPrimaryKeyIndex().update(key1, row1);
    }

    @Test
    public void testProjection(){

        Table table = catalog.getTable("customer");

        ColumnReference column1 = new ColumnReference("c_discount", table);
        ColumnReference column2 = new ColumnReference("c_last",table);
        ColumnReference column3 = new ColumnReference("c_credit", table);

        List<ColumnReference> columnReferenceList = new ArrayList<>(3);
        columnReferenceList.add(column1);
        columnReferenceList.add(column2);
        columnReferenceList.add(column3);

        TypedProjector projector = new TypedProjector(CustomerInfoDTO.class, columnReferenceList);

        Collection<Row> rows = new ArrayList<>(2);
        Collections.addAll(rows, new Row(1F,"1","1" ) );

        RowOperatorResult operatorResult = new RowOperatorResult( rows );

        projector.accept( operatorResult );

        Object object = projector.get();

        assert(object != null);

    }

}
