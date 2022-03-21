package dk.ku.di.dms.vms;

import dk.ku.di.dms.vms.manager.Manager;
import dk.ku.di.dms.vms.manager.ManagerMetadata;
import dk.ku.di.dms.vms.metadata.exception.QueueMappingException;

import java.lang.reflect.InvocationTargetException;

import static java.lang.Thread.sleep;

public class Main {

    // TODO read application config and apply the proper configuration
    public static void main(String[] args) throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, QueueMappingException {

        // periodical task to check whether threads are working correctly
        // https://stackoverflow.com/questions/7814089/how-to-schedule-a-periodic-task-in-java

        //Timer time = new Timer();
        //time.schedule(new HealthChecker(executorService), 0, TimeUnit.SECONDS.toMillis(30));

        // this is for future, production settings. let's go for a simple strategy now
//        HealthChecker healthChecker = new HealthChecker(executorService);
        //final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        //ScheduledFuture future = scheduler.scheduleAtFixedRate(manager, 0, 30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new ShutDownHookThread());

        ManagerMetadata managerMetadata = new ManagerMetadata();

        // the main thread will block
        Manager manager = null;
        try {
            manager = new Manager(managerMetadata);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        // DirectExecutor exec = new DirectExecutor();

        while(true){

            try {
                //CompletableFuture<Void> future = CompletableFuture.runAsync(manager);
                //future.get();
                manager.run();
                // exec.execute(manager);
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

//        Scanner userInput = new Scanner(System.in);
//        System.out.println("Type something to end program.");
//        String input = userInput.nextLine();
        // System.exit(0);

    }

    /**
     * From doc: "However, the Executor interface does not strictly require that
     * execution be asynchronous. In the simplest case, an executor can run the
     * submitted task immediately in the caller's thread"
     */
//    private static class DirectExecutor implements Executor {
//        public void execute(Runnable r) {
//            r.run();
//        }
//    }

    private static class ShutDownHookThread extends Thread {
        @Override
        public void run() {

            // TODO clean up all threads and pending work

            // shutdown in h2 connection manager

            System.out.println("Shut Down Hook Called");
            super.run();
        }
    }

}
