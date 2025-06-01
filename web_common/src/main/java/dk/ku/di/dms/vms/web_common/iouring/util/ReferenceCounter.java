package dk.ku.di.dms.vms.web_common.iouring.util;

public class ReferenceCounter<T> {
    private final T ref;
    private int referenceCount = 0;

    public ReferenceCounter(T ref) {
        this.ref = ref;
    }

    public T ref() {
        return ref;
    }

    public int incrementReferenceCount() {
        return ++referenceCount;
    }

    public int deincrementReferenceCount() {
        return --referenceCount;
    }
}
