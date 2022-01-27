package dk.ku.di.dms.vms.database.store;

import dk.ku.di.dms.vms.database.store.AbstractIndex;
import dk.ku.di.dms.vms.database.store.Column;

public class SingleColumnIndex extends AbstractIndex {

    public Column column;


    @Override
    public int hashCode() {
        return (int) column.hashCode();
    }
}
