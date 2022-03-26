package dk.ku.di.dms.vms.sdk.core.metadata;

public record ForeignKeyReference
        (String vmsTableName,
         String columnName) {}