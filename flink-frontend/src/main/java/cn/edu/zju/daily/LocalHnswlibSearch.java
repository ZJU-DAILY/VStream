package cn.edu.zju.daily;

import cn.edu.zju.daily.data.vector.FloatVector;
import cn.edu.zju.daily.data.vector.FloatVectorIterator;
import cn.edu.zju.daily.data.vector.FvecIterator;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import me.tongfei.progressbar.ProgressBar;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalHnswlibSearch {

    private static final boolean indexBuilt = true;
    private static final Path indexPath = Paths.get("./index.idx");

    private static Index<Long, float[], FloatVector, Float> index;

    private final static int dimension = 128;
    private final static int m = 16;
    private final static int ef = 16;
    private final static int efConstruction = 128;
    private final static int efSearch = 128;
    private final static int maxElements = 40_000_000;
    private final static int k = 50;

    public static void main(String[] args) throws IOException {

        FloatVectorIterator queries = new FloatVectorIterator(
                new FvecIterator(new RandomAccessFile("/home/auroflow/code/vector-search/data/twitter7/queries.fvecs", "r"),
                        1, 0, 1_000_000, FvecIterator.InputType.F_VEC));

        if (!indexBuilt) {
            index = HnswIndex.newBuilder(dimension, DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE, maxElements)
                    .withM(m).withEf(ef).withEfConstruction(efConstruction).build();
            FloatVectorIterator vectors = new FloatVectorIterator(
                    new FvecIterator(new RandomAccessFile("/home/auroflow/code/vector-search/data/twitter7/vectors.fvecs", "r"),
                            1, 0, 5_000_000, FvecIterator.InputType.F_VEC));
            try (ProgressBar bar = new ProgressBar("Indexing", 5_000_000)) {
                for (FloatVector vector : vectors) {
                    index.add(vector);
                    bar.step();
                }
            }
            index.save(indexPath);
        } else {
            index = HnswIndex.load(indexPath);
        }

        for (FloatVector query : queries) {
            System.out.print(query.id());
            index.findNearest(query.vector(), k).forEach(result -> {
                System.out.print(" " + result.item().id());
            });
            System.out.println();
        }
    }
}
