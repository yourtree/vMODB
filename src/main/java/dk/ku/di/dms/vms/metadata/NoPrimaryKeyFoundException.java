package dk.ku.di.dms.vms.metadata;

public class NoPrimaryKeyFoundException extends Exception {
    public NoPrimaryKeyFoundException(String message) {
        super(message);
    }
}
