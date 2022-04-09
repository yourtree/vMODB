package dk.ku.di.dms.vms.modb.common.meta;

public record ForeignKeyReference
        (String vmsTableName,
         String columnName) {}