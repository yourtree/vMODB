package dk.ku.di.dms.vms.tpcc.warehouse.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

public final class WarehouseHttpHandler extends DefaultHttpHandler {

    private final IWarehouseRepository warehouseRepository;

    private final IDistrictRepository districtRepository;

    private final ICustomerRepository customerRepository;

    public WarehouseHttpHandler(ITransactionManager transactionManager,
                                IWarehouseRepository warehouseRepository,
                                IDistrictRepository districtRepository,
                                ICustomerRepository customerRepository) {
        super(transactionManager);
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
    }

    @Override
    public void post(String uri, String payload) {
        String[] uriSplit = uri.split("/");
        String table = uriSplit[uriSplit.length - 1];
        switch (table){
            case "warehouse" -> {
                Warehouse warehouse = SERDES.deserialize(payload, Warehouse.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.warehouseRepository.upsert(warehouse);
            }
            case "district" -> {
                District district = SERDES.deserialize(payload, District.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.districtRepository.upsert(district);
            }
            case "customer" -> {
                Customer customer = SERDES.deserialize(payload, Customer.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.customerRepository.upsert(customer);
            }
        }
    }

}
