package dk.ku.di.dms.vms.sidecar.event;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

public record EventExample(
     int c_w_id,
     int c_d_id,
     int c_id) implements IEvent {}
