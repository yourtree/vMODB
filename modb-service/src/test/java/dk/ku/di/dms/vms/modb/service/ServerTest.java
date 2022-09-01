package dk.ku.di.dms.vms.modb.service;

import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

public class ServerTest {


    @Test
    public void test() throws IOException {

//        SidecarPubSub pubSub = SidecarPubSub.newInstance();
//        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
//
//        AsyncVMSServer vmsServer = new AsyncVMSServer(pubSub.inputQueue, pubSub.outputQueue, serdes);


        // TODO create a dumb client to connect and test the handshake...

        AsynchronousSocketChannel eventChannel = AsynchronousSocketChannel.open();

        // localhost port 80
//        eventChannel.connect(  );


    }

}
