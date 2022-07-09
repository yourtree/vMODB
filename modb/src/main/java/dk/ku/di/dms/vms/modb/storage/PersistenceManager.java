package dk.ku.di.dms.vms.modb.storage;

import dk.ku.di.dms.vms.modb.catalog.Catalog;
import dk.ku.di.dms.vms.modb.schema.Row;
import dk.ku.di.dms.vms.modb.table.Table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Often while get the records and persist in the memory
 */
public class PersistenceManager { // runnable?

    private Object _lock;

    private ExecutorService executorService; // it receives a executor service with appropriate number of thread pool

    // Catalog of the respective vms
    private Catalog catalog; // can get the PK of every table

    // how to make the persistence task highly scalable? make independent tasks, avoid coordination.
    // should provide a file map for each table then....a lot of small files, low granularity

    // TODO can we get a better design to log the modified rows?
    //  maybe every transaction must asynchronously push their changes to here...
    private Map<Table, Set<Row>> map = new HashMap<>();

    public PersistenceManager(){

        this._lock = new Object();

    }

    public void markRowForLogging(Table table, Row row){
        if (map.containsKey( table )){
            map.get(table).add(row);
            return;
        }
        HashSet<Row> set = new HashSet<>();
        set.add( row );
        map.put(table, set);
    }

//    public Future<Boolean> run(){
//
//        // get the delta of primary indexes of all tables
//
//        // build tasks
//
//        synchronized (_lock){
//
//
//
//        }
//
//        return null;
//
//    }

//    private class PersistenceDaemon implements Callable<Boolean> {
//
//        public PersistenceDaemon(AbstractIndex<IKey> pk){
//
//        }
//
//        @Override
//        public Boolean call() throws Exception {
//            return null;
//        }
//
//    }
//
//    public Future<Boolean> commit(){
//
//        // mark non-committed records as committed. use an index for fast access. ranges are better...
//
//
//        return null;
//    }

    // how to store data?

}
