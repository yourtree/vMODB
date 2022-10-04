package dk.ku.di.dms.vms.playground.micro_tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class App {

    protected static final Logger logger = Logger.getLogger("App");

    public static void main( String[] args ) throws IOException, ExecutionException, InterruptedException {

        // initialize virtual microservices
        // cannot initialize them here otherwise the events get mixed

        // bulk data load if necessary
        DataLoader dataLoader = new DataLoader();

        dataLoader.start();

        // start generating new order transaction inputs


        // process inputs processed and convert into input that coordinator understands



    }

}
