package dk.ku.di.dms.vms.modb.common.constraint;

public record ForeignKeyReference
        (String vmsTableName, // this is always part of the same virtual microservice
         String columnName) {}