package dk.ku.di.dms.vms.modb.common.meta;

public record ForeignKeyReference
        (String vmsTableName, // this can be external virtual microservices
         String columnName) {}