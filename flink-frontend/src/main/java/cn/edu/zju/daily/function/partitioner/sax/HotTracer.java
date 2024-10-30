package cn.edu.zju.daily.function.partitioner.sax;

import static java.util.stream.Collectors.toSet;

import java.util.*;
import org.apache.commons.lang3.tuple.MutablePair;

/** Hot tracer. */
public class HotTracer {

    private final int size;
    private final int k;

    private final int[] counts;

    /** hot key -> count, sorted by count in ascending order */
    private final LinkedList<MutablePair<Integer, Integer>> hotList;

    public HotTracer(int size, int k) {
        this.size = size;
        this.k = k;
        this.counts = new int[size];
        this.hotList = new LinkedList<>();
    }

    public void add(int key) {
        if (key >= size) {
            throw new IllegalArgumentException("key " + key + " out of range " + size);
        }
        counts[key]++;

        // If key is not hot, no need to do anything
        if (!isHot(key)) {
            return;
        }

        // If key is hot, update hot list
        ListIterator<MutablePair<Integer, Integer>> it = updateKeyInHotList(key);
        position(it);
        if (hotList.size() > k) {
            hotList.removeFirst();
        }
    }

    public Set<Integer> getHotKeys() {
        return hotList.stream().map(MutablePair::getLeft).collect(toSet());
    }

    public boolean isHot(int key) {
        return hotList.size() < k || counts[key] >= hotList.get(0).getRight();
    }

    public void clear() {
        Arrays.fill(counts, 0);
        hotList.clear();
    }

    LinkedList<MutablePair<Integer, Integer>> getHotList() {
        return hotList;
    }

    /**
     * Get the iterator in hot list for the key. If not found, add to the first.
     *
     * @param key key
     * @return the iterator whose 'next' is the key provided
     */
    private ListIterator<MutablePair<Integer, Integer>> updateKeyInHotList(int key) {
        ListIterator<MutablePair<Integer, Integer>> it = hotList.listIterator();
        while (it.hasNext()) {
            MutablePair<Integer, Integer> pair = it.next();
            if (pair.getLeft() == key) {
                pair.setRight(counts[key]);
                it.previous();
                return it;
            }
        }

        // Not found: add to first
        MutablePair<Integer, Integer> pair = MutablePair.of(key, counts[key]);
        hotList.addFirst(pair);
        return hotList.listIterator();
    }

    private void position(ListIterator<MutablePair<Integer, Integer>> it) {
        while (true) {
            if (!it.hasNext()) {
                return;
            }
            MutablePair<Integer, Integer> el1 = it.next();
            if (!it.hasNext()) {
                it.previous();
                return;
            }
            MutablePair<Integer, Integer> el2 = it.next();
            if (el1.getRight() <= el2.getRight()) {
                it.previous();
                it.previous();
                return;
            }
            it.previous();
            it.set(el1);
            it.previous();
            it.set(el2);
            it.next();
        }
    }
}
