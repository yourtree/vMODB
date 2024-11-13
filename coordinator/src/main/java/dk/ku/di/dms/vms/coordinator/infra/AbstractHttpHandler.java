package dk.ku.di.dms.vms.coordinator.infra;

import dk.ku.di.dms.vms.coordinator.Coordinator;

public abstract class AbstractHttpHandler {

    protected final Coordinator coordinator;

    public AbstractHttpHandler(Coordinator coordinator){
        this.coordinator = coordinator;
    }

    protected byte[] getNumTIDsCommittedBytes() {
        long lng = this.coordinator.getNumTIDsCommitted();
        System.out.println("Number of TIDs committed: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    protected byte[] getNumTIDsSubmittedBytes() {
        long lng = this.coordinator.getNumTIDsSubmitted();
        System.out.println("Number of TIDs submitted: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    public byte[] getAsBytes(String uri) {
        String[] uriSplit = uri.split("/");
        if (uriSplit[1].equals("status")) {
            if(uriSplit[2].equals("committed")) {
                return this.getNumTIDsCommittedBytes();
            }
            return this.getNumTIDsSubmittedBytes();
        }
        return null;
    }

}
