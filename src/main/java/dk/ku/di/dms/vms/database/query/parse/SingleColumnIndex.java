package dk.ku.di.dms.vms.database.query.parse;

import dk.ku.di.dms.vms.database.store.Column;

public class SingleColumnIndex extends dk.ku.di.dms.vms.store.AbstractIndex {

    public Column column;


    @Override
    public int hashCode() {
        return (int) column.hashCode();
    }
}
