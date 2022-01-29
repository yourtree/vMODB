package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.Column;

import java.util.List;

public class CompositeIndex extends AbstractIndex {

    public final List<Column> columns;
    public final int hashCode;

    public CompositeIndex(List<Column> columns) {
        this.columns = columns;

        // E.g., byte aByte = (byte)0b00100001;
        StringBuilder sb = new StringBuilder("0b");
        for(Column col : columns){
            sb.append(col.hashCode);
        }
        Byte byte_ = Byte.decode(sb.toString());
        hashCode = byte_.hashCode();

    }

    @Override
    public int hashCode() {
        return hashCode;
    }

}
