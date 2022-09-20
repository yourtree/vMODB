package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.planner.Planner;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;

public record ModbModules(
        VmsRuntimeMetadata vmsRuntimeMetadata,
        Catalog catalog,
        Analyzer analyzer,
        Planner planner
){}
