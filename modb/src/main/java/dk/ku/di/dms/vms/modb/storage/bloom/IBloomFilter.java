package dk.ku.di.dms.vms.modb.storage.bloom;

/**
 * Basic interface, extracted from: {https://richardstartin.github.io/posts/building-a-bloom-filter-from-scratch}
 * Link: https://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 *
 * The idea is to use a bloom filter to keep what records have been modified so far
 *
 * That avoids having to copy elements to the actual memory space on every write
 * Strategies like keeping the last value written (or the respective columns only)
 * in the heap memory or only copying to an intermediate memory space (so only the
 * final checkpointed value is written to actual memory space) are feasible.
 *
 * Bloom filter, elements cannot be remove. That makes the case for resetting the bloom filter at every checkpoint
 *
 * Other DTs that coiuld be useful: https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch
 * Skip list: https://web.stanford.edu/class/archive/cs/cs106b/cs106b.1172/lectures/29-EsotericDataStructures/EsotericDataStructures.pdf
 *
 */
public interface IBloomFilter {

    void add(int value);

    boolean mightContain(int value);

}
