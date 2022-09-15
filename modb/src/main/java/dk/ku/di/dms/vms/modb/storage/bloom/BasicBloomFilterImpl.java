package dk.ku.di.dms.vms.modb.storage.bloom;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;

public class BasicBloomFilterImpl implements IBloomFilter {

    // bit array of size M
    private boolean[] array;

    // the number of bits
    private final int M = Integer.SIZE; // an integer has 32 bits

    // k independent hash functions in the range {1,..,M}
    // k == 2 gives false positive rate under 10% for all M/N > 5
    // that means n = 160
    // tpcc has 100000 items... if contention is 0.1% that is 1000
    // this is not enough, we need one more hash function
    private List<ToIntBiFunction<Integer,Integer>> hashFunctions;

    private static final ToIntBiFunction<Integer,Integer> hash0 = (value, size) -> value % size;

    /*
     * extracted from here: https://www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation/
     */
    private static final ToIntBiFunction<Integer,Integer> hash1 = (value, size) -> {
        int hash = 0;
        for (int i = 0; i < size; i++)
        {
            hash = (hash + getBit(value,i));
            hash = hash % size;
        }
        return hash;
    };

    private static int getBit(int n, int k) {
        return (n >> k) & 1;
    }

    public BasicBloomFilterImpl(){}

    @Override
    public void add(int value) {
        // iterate over the hash functions and install the positive bits
    }

    @Override
    public boolean mightContain(int value) {
        return false;
    }

}
