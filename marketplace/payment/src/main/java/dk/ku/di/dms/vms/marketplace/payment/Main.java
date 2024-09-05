package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.infra.HttpServerJdk;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

public final class Main {

    public static void main(String[] ignored) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                Constants.PAYMENT_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.payment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        HttpServerJdk.init(vms, "/payment", Constants.PAYMENT_HTTP_PORT);
        System.out.println("Payment: JDK HTTP Server started");
    }

}
