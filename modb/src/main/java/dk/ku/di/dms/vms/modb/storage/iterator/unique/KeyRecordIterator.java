package dk.ku.di.dms.vms.modb.storage.iterator.unique;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public final class KeyRecordIterator implements IRecordIterator<IKey> {

    public final IKey[] keys;

    public final ReadOnlyIndex<IKey> index;

    private int position;

    private final int numberOfKeys;

    private IKey currentKey;

    private long currentAddress;

    public KeyRecordIterator(ReadOnlyIndex<IKey> index, IKey[] keys){
        this.index = index;
        this.keys = keys;

        // initialize the first
        this.position = 0;
        this.numberOfKeys = keys.length;
        this.currentKey = keys[position];
        this.currentAddress = index.address(currentKey);
    }

    @Override
    public boolean hasElement() {
        while(!index.exists(currentAddress) && position < numberOfKeys){
            position++;
            currentKey = keys[position];
            currentAddress = index.address(currentKey);
        }
        return index.exists(currentAddress);
    }

    @Override
    public void next() {
        position++;
        currentKey = keys[position];
        currentAddress = index.address(currentKey);
    }

    @Override
    public IKey get(){
        return this.currentKey;
    }

    @Override
    public long address(){
        return this.currentAddress;
    }

}
