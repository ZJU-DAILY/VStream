package cn.edu.zju.daily.data.result;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a (partial or complete) search result.
 */
public class SearchResult implements Serializable {

    private final int nodeId;
    private final long queryId;
    private final int numPartitionsToCombine;
    private final int numPartitionsCombined;
    private final List<Long> ids;
    private final List<Float> distances;  // ascending
    private final long queryEventTime;
    private long searchCompleteTime;

    public SearchResult(long queryId, List<Long> ids, List<Float> distances) {
        this(-1, queryId, ids, distances, 1, 1, 0L);
    }

    public SearchResult(int nodeId, long queryId, List<Long> ids, List<Float> distances, int numPartitionsCombined, int numPartitionsToCombine, long queryEventTime) {
        assert(ids.size() == distances.size());
        this.nodeId = nodeId;
        this.queryId = queryId;
        this.ids = ids;
        this.distances = distances;
        this.numPartitionsCombined = numPartitionsCombined;
        this.numPartitionsToCombine = numPartitionsToCombine;
        this.queryEventTime = queryEventTime;
        this.searchCompleteTime = 0L;
    }

    public void sort() {
        List<Long> ids = new ArrayList<>(this.ids);
        List<Float> distances = new ArrayList<>(this.distances);
        this.ids.clear();
        this.distances.clear();
        while (!ids.isEmpty()) {
            int minIndex = 0;
            for (int i = 1; i < ids.size(); i++) {
                if (distances.get(i) < distances.get(minIndex) || (Objects.equals(distances.get(i),
                        distances.get(minIndex)) && ids.get(i) < ids.get(minIndex))) {
                    minIndex = i;
                }
            }
            this.ids.add(ids.remove(minIndex));
            this.distances.add(distances.remove(minIndex));
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getQueryId() {
        return queryId;
    }

    public int size() {
        return ids.size();
    }

    public long id(int i) {
        return ids.get(i);
    }

    public float distance(int i) {
        return distances.get(i);
    }

    public boolean isComplete() {
        return numPartitionsCombined == numPartitionsToCombine;
    }

    public int getCopies() {
        return this.numPartitionsToCombine;
    }

    public List<Long> getIds() {
        return ids;
    }

    private void append(long id, float distance) {
        // assuming that the distances are sorted in ascending order, and that the inserted distance is no smaller than the
        // last distance in the array
        if (ids.isEmpty() || ids.get(ids.size() - 1) != id) {
            ids.add(id);
            distances.add(distance);
        }
    }

    private static long mergeQueryId(long id1, long id2) {
        if (id1 == -1) {
            return id2;
        } else if (id2 == -1) {
            return id1;
        } else {
            return id1;
        }
    }

    public long getQueryEventTime() {
        return queryEventTime;
    }

    public long getSearchDuration() {
        if (searchCompleteTime == 0L) return 0L;
        return searchCompleteTime - queryEventTime;
    }

    /**
     * Combines two search results into one.
     *
     * @param a
     * @param b
     * @param k number of results to keep
     * @return
     */
    public static SearchResult combine(SearchResult a, SearchResult b, int k) {
        Preconditions.checkArgument(a.queryId == -1 || b.queryId == -1 || a.queryId == b.queryId);
        Preconditions.checkArgument(a.numPartitionsToCombine == b.numPartitionsToCombine);
        Preconditions.checkArgument(a.queryEventTime == b.queryEventTime);
        // combine the two lists
        SearchResult r = new SearchResult(-1, mergeQueryId(a.queryId, b.queryId), new ArrayList<>(k), new ArrayList<>(k),
                a.numPartitionsCombined + b.numPartitionsCombined, a.numPartitionsToCombine, a.queryEventTime);
        int i = 0;
        int j = 0;

        while (r.size() < k && (i < a.size() || j < b.size())) {
            if (i < a.size() && j < b.size()) {
                if (a.distance(i) < b.distance(j) || (Objects.equals(a.distance(i), b.distance(j)) && a.id(i) < b.id(j))) {
                    r.append(a.id(i), a.distance(i));
                    i++;
                } else {
                    r.append(b.id(j), b.distance(j));
                    j++;
                }
            } else if (i < a.size()) {
                r.append(a.id(i), a.distance(i));
                i++;
            } else {
                r.append(b.id(j), b.distance(j));
                j++;
            }
        }

        if (r.isComplete()) {
            r.searchCompleteTime = System.currentTimeMillis();
        }
        return r;
    }

    public static float getAccuracy(SearchResult result, SearchResult groundTruth) {
        int k = result.size();
        int count = 0;
        for (long id : result.ids) {
            if (groundTruth.ids.contains(id)) {
                count++;
            }
        }
        return (float) count / k;
    }

    @Override
    public String toString() {
        return "SearchResult{" +
                "nodeId=" + nodeId +
                ", queryId=" + queryId +
                ", numPartitionsToCombine=" + numPartitionsToCombine +
                ", numPartitionsCombined=" + numPartitionsCombined +
                ", ids=" + ids +
                ", distances=" + distances +
                ", queryEventTime=" + queryEventTime +
                ", searchCompleteTime=" + searchCompleteTime +
                '}';
    }
}
