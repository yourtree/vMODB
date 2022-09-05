package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

import java.nio.ByteBuffer;

/**
 * Contains:
 * - the transaction that have been modified a data item
 * - the index
 * - the key (in the index)
 * - the change/value (after-write)
 *
 */
public class VersionNode {

    private VersionNode(){}

    private long tid;

    private AbstractIndex<IKey> index;

    // identifier in the original index
    private IKey key;

    private Operation operation;

    /** two below fields are ony filled if it is update */
    // a transaction can make changes to several data items
    // here the granularity is of a column
    private int columnIndex;

    // the value
    private Object value;

    // only filled if it is insert
    private ByteBuffer buffer;

    private static VersionNode update(long tid, AbstractIndex<IKey> index, IKey key, int columnIndex, Object value){
        VersionNode versionNode = new VersionNode();
        versionNode.operation = Operation.UPDATE;
        versionNode.tid = tid;
        versionNode.index = index;
        versionNode.key = key;
        versionNode.columnIndex = columnIndex;
        versionNode.value = value;
        return versionNode;
    }

    private static VersionNode insert(long tid, AbstractIndex<IKey> index, IKey key, ByteBuffer buffer){
        VersionNode versionNode = new VersionNode();
        versionNode.operation = Operation.INSERT;
        versionNode.tid = tid;
        versionNode.index = index;
        versionNode.buffer = buffer;
        return versionNode;
    }

    private static VersionNode delete(long tid, AbstractIndex<IKey> index, IKey key){
        VersionNode versionNode = new VersionNode();
        versionNode.operation = Operation.DELETE;
        versionNode.tid = tid;
        versionNode.index = index;
        versionNode.key = key;
        return versionNode;
    }

}
