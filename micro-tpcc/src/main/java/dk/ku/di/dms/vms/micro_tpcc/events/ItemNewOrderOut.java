package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

import java.util.Map;

public record ItemNewOrderOut(Map<Integer,Float> itemsPrice) implements IEvent {}
