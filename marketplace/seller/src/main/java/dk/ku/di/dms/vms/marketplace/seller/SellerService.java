package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Subscribe;

@Microservice("seller")
public class SellerService {



    @Subscribe("invoice_issued")
    public void processInvoiceIssued(InvoiceIssued invoiceIssued){

    }

    @Subscribe("shipment_notification")
    public void processShipmentNotification(ShipmentNotification shipmentNotification){

    }

}
