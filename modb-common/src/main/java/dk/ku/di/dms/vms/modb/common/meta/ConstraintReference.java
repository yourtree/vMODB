package dk.ku.di.dms.vms.modb.common.meta;

public class ConstraintReference {

    public final ConstraintEnum constraint;

    // for now only supporting one column, but a constraint may apply to several colums
    public final int column;

    public ConstraintReference(ConstraintEnum constraint, int column) {
        this.constraint = constraint;
        this.column = column;
    }
}
