package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.Column;

public class SingleIndex extends AbstractIndex {

    public Column column;

    @Override
    public int hashCode() {
        return column.hashCode();
    }

}
