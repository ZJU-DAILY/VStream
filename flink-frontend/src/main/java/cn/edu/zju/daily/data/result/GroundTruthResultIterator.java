package cn.edu.zju.daily.data.result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GroundTruthResultIterator implements Iterator<SearchResult> {

    private final int k;
    private final IvecIterator it;
    private int count = 0;

    public GroundTruthResultIterator(IvecIterator it, int k) {
        this.it = it;
        this.k = k;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public SearchResult next() {
        int[] arr = it.next();
        List<Long> ids = new ArrayList<>();
        List<Float> distances = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            ids.add((long) arr[i]);
            distances.add(0f);
        }
        return new SearchResult(-1, count++, ids, distances, 1, 1, 0);
    }

    public static GroundTruthResultIterator fromFile(String filename, int k) throws Exception {
        return new GroundTruthResultIterator(IvecIterator.fromFile(filename), k);
    }
}
