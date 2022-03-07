package dk.ku.di.dms.vms.database.store.meta;

public class ConstraintReference {

    private final ConstraintEnum constraint;

    // for now only supporting one column, but a constraint may apply to several colums
    private final int column;

    public ConstraintReference(ConstraintEnum constraint, int column) {
        this.constraint = constraint;
        this.column = column;
    }
}
