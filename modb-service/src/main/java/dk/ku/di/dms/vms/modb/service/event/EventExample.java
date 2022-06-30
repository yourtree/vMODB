package dk.ku.di.dms.vms.modb.service.event;

import dk.ku.di.dms.vms.modb.common.event.IVmsApplicationEvent;

public record EventExample(
     int c_w_id,
     int c_d_id,
     int c_id) implements IVmsApplicationEvent {}
