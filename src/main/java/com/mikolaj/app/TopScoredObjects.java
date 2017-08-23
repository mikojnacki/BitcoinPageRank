package com.mikolaj.app;

import org.apache.hadoop.util.PriorityQueue;

/**
 *  Based on https://github.com/lintool/tools/blob/master/lintools-datatypes/src/main/java/tl/lin/data/queue/TopScoredObjects.java
 * Modified by Mikolaj Chojnacki
 * Credits to Jimmy Lin @lintool
 */
public class TopScoredObjects<K extends Comparable<K>> {
    private class ScoredObjectPriorityQueue extends PriorityQueue<PairOfObjectFloat<K>> {

        private ScoredObjectPriorityQueue(int maxSize) {
            super.initialize(maxSize);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean lessThan(Object obj0, Object obj1) {
            // If equal scores, break tie by object descending.
            if (((PairOfObjectFloat<K>) obj0).getRightElement() == ((PairOfObjectFloat<K>) obj1)
                    .getRightElement()) {
                return ((PairOfObjectFloat<K>) obj0).getLeftElement().compareTo(
                        ((PairOfObjectFloat<K>) obj1).getLeftElement()) < 0 ? true : false;
            }

            return ((PairOfObjectFloat<K>) obj0).getRightElement() < ((PairOfObjectFloat<K>) obj1)
                    .getRightElement() ? true : false;
        }
    }

    private final ScoredObjectPriorityQueue queue;
    private final int maxElements;

    // TODO: Add the option to control how to break scoring ties

    public TopScoredObjects(int n) {
        queue = new ScoredObjectPriorityQueue(n);
        maxElements = n;
    }

    public void add(K obj, float f) {
        queue.insert(new PairOfObjectFloat<K>(obj, f));
    }

    public int getMaxElements() {
        return maxElements;
    }

    public int size() {
        return queue.size();
    }

    @SuppressWarnings("unchecked")
    public PairOfObjectFloat<K>[] extractAll() {
        int len = queue.size();
        PairOfObjectFloat<K>[] arr = (PairOfObjectFloat<K>[]) new PairOfObjectFloat[len];
        for (int i = 0; i < len; i++) {
            arr[len - 1 - i] = queue.pop();
        }
        return arr;
    }
}