package dk.ku.di.dms.vms.modb.common.data_structure;

/**
 * Basic entry to represent pair of values
 *
 * @param <T1> value 1
 * @param <T2> value 2
 */
public record Tuple<T1, T2>(T1 t1, T2 t2) { }