package dk.ku.di.dms.vms.modb.transaction.internal;

/**
 * A data structure that serves the only purpose of storing historical writes of a key
 * regarding an index. Assumptions:
 * (i) It assumes there will be only one writer at every single time.
 * (ii) It also assumes new entries are appended to the head.
 * (iii) Readers never read from the head, unless that task is completed (all-or-nothing atomicity).
 * These three assumptions make it safe to call {@link #removeUpToEntry}
 * concurrently with a writer, as long as the keys being inserted, removed, and read do not
 * intersect. In other words, concurrent threads are supposed to always operate on
 * distinct partitions of the data structure.
 * @param <K> {@link dk.ku.di.dms.vms.modb.common.transaction.TransactionId}
 * @param <V> {@link dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite}
 */
public final class SingleWriterMultipleReadersFIFO<K extends Comparable<K>,V> {

    public static class Entry<K,V> {
        private final K key;
        private final V val;
        private volatile Entry<K,V> next;

        public Entry(K key, V val, Entry<K,V> next){
            this.key = key;
            this.val = val;
            this.next = next;
        }

        public K key(){
            return this.key;
        }

        public V val(){
            return this.val;
        }

    }

    private volatile Entry<K,V> first;

    public void put(K key, V val){
        // always insert in the front
        Entry<K,V> currFirst = this.first;
        this.first = new Entry<>(key, val, currFirst);
    }

    /**
     * Removes the head of the data structure
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public void poll(){
        var next = this.first.next;
        this.first = next;
    }

    /**
     * Gets the entry corresponding to the specified key; if no such entry
     * exists, returns the entry for the greatest key less than the specified
     * key; if no such entry exists, returns {@code null}.
     */
    public Entry<K,V> floorEntry(K key) {
        if(this.first == null) return null;
        Entry<K,V> curr = this.first;
        int cmp = curr.key.compareTo(key);
        while(cmp > 0){
            curr = curr.next;
            if(curr == null) break;
            cmp = curr.key.compareTo(key);
        }
        if(cmp <= 0) return curr;
        return null;
    }

    /**
     * Gets the entry for the lowest key higher than the specified
     * key; if no such entry exists, returns {@code null}.
     * In other words, gets the immediate successor of key.
     */
    public Entry<K,V> getHigherEntry(K key) {
        if(this.first == null) return null;
        Entry<K,V> curr = this.first;
        Entry<K,V> prev = null;
        int cmp = curr.key.compareTo(key);
        while(cmp > 0){
            prev = curr;
            curr = curr.next;
            if(curr == null) break;
            cmp = curr.key.compareTo(key);
        }
        return prev;
    }

    /**
     * Remove all entries below or equal the key
     * Method is used to remove TIDs that cannot be seen anymore
     * @param key node identifier
     */
    public Entry<K,V> removeUpToEntry(K key){
        final Entry<K,V> entryToReturn = this.getHigherEntry(key);
        Entry<K,V> currFloorEntry = entryToReturn;
        Entry<K,V> auxEntry;
        while(currFloorEntry != null){
            auxEntry = currFloorEntry.next;
            // set next to null to lose reference
            currFloorEntry.next = null;
            currFloorEntry = auxEntry;
        }
        return entryToReturn;
    }

    public void clear(){
        var current = this.first;
        if(current == null) return;
        var next = current.next;
        while (next != null){
            current = next;
            next = current.next;
        }
    }

    @Override
    public String toString(){
        var current = this.first;
        if(current == null) return "";
        String lineSeparator = System.lineSeparator();
        StringBuilder sb = new StringBuilder();
        while (current != null){
            sb.append( current.key.toString() )
                    .append(" : ")
                    .append(current.val)
                    .append(lineSeparator);
            current = current.next;
        }
        return sb.toString();
    }

}
