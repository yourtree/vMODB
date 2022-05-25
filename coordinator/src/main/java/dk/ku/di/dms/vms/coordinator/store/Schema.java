package dk.ku.di.dms.vms.coordinator.store;

public class Schema {

    public long committedOffset;

    // number of transactions registered
    public int numTransactions;

    public static class TransactionSchema {

        // the size of the bytebuffer representing the complex object
        public int size;

        // DAG, json form
        public byte[] object;

    }

    // number of VMSs registered
    public int numVMSs;

    public static class VMSMetadata {

        // the size of the bytebuffer representing the
        public long lastOffset;

        // string, FIXED SIZE
        public byte[] name;

        // the size of the bytebuffer representing the complex object
        public int dataSchemaSize;

        // json
        public byte[] dataSchemaObject;

        public int eventSchemaSize;

        // json
        public byte[] eventSchemaObject;

    }

}
