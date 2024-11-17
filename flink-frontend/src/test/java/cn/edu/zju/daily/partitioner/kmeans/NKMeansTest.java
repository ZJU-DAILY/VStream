package cn.edu.zju.daily.partitioner.kmeans;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class NKMeansTest {

    @Test
    void test() {
        NKMeans nkmeans =
                NKMeans.fit(
                        new double[][] {
                            {0, 0}, {0, 1}, {1, 0}, {4, 5}, {5, 4}, {5, 5}, {-4, -5}, {-5, -4},
                            {-5, -5}
                        },
                        3,
                        10);
        System.out.println(Arrays.deepToString(nkmeans.centroids));
        List<Integer> nearest = nkmeans.nearest(new double[] {1, 1}, 2);
        System.out.println(nearest);
    }
}
