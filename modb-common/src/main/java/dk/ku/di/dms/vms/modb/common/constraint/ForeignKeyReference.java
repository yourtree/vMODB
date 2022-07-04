package dk.ku.di.dms.vms.modb.common.constraint;

public record ForeignKeyReference
        (String vmsTableName, // this can be external virtual microservices
         String columnName) {}