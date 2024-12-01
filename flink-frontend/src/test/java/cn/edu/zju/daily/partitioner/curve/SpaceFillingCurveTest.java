package cn.edu.zju.daily.partitioner.curve;

import java.math.BigInteger;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class SpaceFillingCurveTest {

    private static final int dim = 20;
    private static final int bits = 4;
    private long[] vector = new long[dim];

    private static final Random random = new Random(38324);

    @Test
    void testHilbert() {
        int maxValue = (int) Math.pow(2, bits);

        for (int i = 0; i < dim; i++) {
            vector[i] = random.nextInt(maxValue);
        }

        SpaceFillingCurve.Builder builder = new HilbertCurve.Builder();
        builder.setDimension(dim).setBits(bits);
        SpaceFillingCurve curve = builder.build();

        BigInteger index = curve.index(vector);
        System.out.println("Hilbert index: " + index);
    }

    @Test
    void testZOrder() {
        int maxValue = (int) Math.pow(2, bits);

        for (int i = 0; i < dim; i++) {
            vector[i] = random.nextInt(maxValue);
        }

        SpaceFillingCurve.Builder builder = new ZOrderCurve.Builder();
        builder.setDimension(dim).setBits(bits);
        SpaceFillingCurve curve = builder.build();

        BigInteger index = curve.index(vector);
        System.out.println("Z-order index: " + index);
    }

    @Test
    void testPeano() {
        int maxValue = (int) Math.pow(3, bits);

        for (int i = 0; i < dim; i++) {
            vector[i] = random.nextInt(maxValue);
        }

        SpaceFillingCurve.Builder builder = new PeanoCurve.Builder();
        builder.setDimension(dim).setBits(bits);
        SpaceFillingCurve curve = builder.build();

        BigInteger index = curve.index(vector);
        System.out.println("Peano index: " + index);
    }

    @Test
    void testGray() {
        int maxValue = (int) Math.pow(2, bits);

        for (int i = 0; i < dim; i++) {
            vector[i] = random.nextInt(maxValue);
        }

        SpaceFillingCurve.Builder builder = new GrayCurve.Builder();
        builder.setDimension(dim).setBits(bits);
        SpaceFillingCurve curve = builder.build();

        BigInteger index = curve.index(vector);
        System.out.println("Gray index: " + index);
    }
}
