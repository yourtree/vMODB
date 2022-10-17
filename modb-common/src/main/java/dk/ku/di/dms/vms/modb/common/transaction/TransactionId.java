package dk.ku.di.dms.vms.modb.common.transaction;

public record TransactionId ( long tid, int identifier ) implements Comparable<TransactionId> {

    @Override
    public int compareTo(TransactionId anotherTransactionId) {
        return compare(this, anotherTransactionId);
    }

    private static int compare(TransactionId x, TransactionId y) {
        return
                 ( (x.tid() > y.tid()) || (x.tid() == y.tid() && x.identifier() > y.identifier()) ) ? 1 :
                         ( (x.tid() < y.tid()) || (x.tid() == y.tid() && x.identifier() < y.identifier()) ) ? -1 :
                        0;

    }

}
