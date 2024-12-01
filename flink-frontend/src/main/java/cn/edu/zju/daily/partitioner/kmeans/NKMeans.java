package cn.edu.zju.daily.partitioner.kmeans;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import smile.clustering.KMeans;

@Slf4j
public class NKMeans extends KMeans {

    public NKMeans(KMeans kmeans) {
        super(kmeans.distortion, kmeans.centroids, kmeans.y);
    }

    public List<Integer> nearest(double[] q, int k) {
        PriorityQueue<Pair<Integer, Double>> pq =
                new PriorityQueue<>(
                        k,
                        Comparator.<Pair<Integer, Double>>comparingDouble(Pair::getRight)
                                .reversed());
        for (int i = 0; i < centroids.length; i++) {
            double dist = distance(centroids[i], q);
            pq.add(Pair.of(i, dist));
            if (pq.size() > k) {
                pq.poll();
            }
        }
        List<Integer> indices = new ArrayList<>(k);
        while (!pq.isEmpty()) {
            indices.add(pq.poll().getLeft());
        }
        return indices;
    }

    public static NKMeans fit(double[][] data, int k, int maxIter) {
        KMeans kmeans = KMeans.fit(data, k, maxIter, 1E5);
        return new NKMeans(kmeans);
    }

    @Override
    protected double distance(double[] x, double[] y) {
        double sum = 0.0;
        for (int i = 0; i < Math.min(10, x.length); i++) {
            double d = x[i] - y[i];
            sum += d * d;
        }

        return sum;
    }
}
