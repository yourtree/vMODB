package dk.ku.di.dms.vms.modb.common.event;

import java.io.Serializable;

/**
 * It serves to identify application-generated events
 * Also modeled to distinguish from system-generated events
 */
public interface IApplicationEvent extends Serializable {}