package dk.ku.di.dms.vms.database.store.refac;

import dk.ku.di.dms.vms.database.store.refac.Table;

import java.util.List;

public abstract class Row {

    // public Table table;

    // collection of values
    private int[] data;

    private int primaryKey;

    private int[] intData;
    private long[] longData;
    private float[] floatData;
    private double[] doubleData;
    private char[] charData;
    private String[] stringData;


    @Override
    public int hashCode() {
        return primaryKey;
    }
}
