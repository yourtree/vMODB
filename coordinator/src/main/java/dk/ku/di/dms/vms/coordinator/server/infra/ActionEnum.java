package dk.ku.di.dms.vms.coordinator.server.infra;

public enum ActionEnum {

    BATCH_REPLICATION_FAILED,

    BATCH_COMMIT,

    BATCH_ABORT,

    ABORT // individual transaction

}
