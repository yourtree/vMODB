package dk.ku.di.dms.vms.modb.common.constraint;

public class ConstraintReference {

    public final ConstraintEnum constraint;

    // for now only supporting one column, but a constraint may apply to several columns
    public final int column;

    public ConstraintReference(ConstraintEnum constraint, int column) {
        this.constraint = constraint;
        this.column = column;
    }
}
