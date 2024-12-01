package cn.edu.zju.daily.partitioner.curve;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * N-dimensional Peano Curve.
 *
 * @see <a href="https://core.ac.uk/download/pdf/82469366.pdf">Space Filling Curves and Mathematical
 *     Programming</a>
 */
@Slf4j
public class PeanoCurve implements SpaceFillingCurve {

    private final int dimension;
    private final int bits; // number of ternary bits
    private final long maxValue;
    private final int[] resultBuffer;
    private final int[][] inputBuffer;
    private final List<Integer> items = new ArrayList<>();

    private PeanoCurve(SpaceFillingCurve.Builder builder) {
        this.dimension = builder.getDimension();
        this.bits = builder.getBits();
        this.maxValue = (long) Math.pow(3, bits);
        this.resultBuffer = new int[dimension * bits];
        this.inputBuffer = new int[dimension][bits];
    }

    @Override
    public BigInteger index(long[] vector) {
        for (long value : vector) {
            if (value >= maxValue) {
                throw new IllegalArgumentException("Vector value out of range.");
            }
            for (int bit = 0; bit < bits; bit++) {
                inputBuffer[0][bits - bit - 1] = (int) Math.floorMod(value, 3);
                value = Math.floorDiv(value, 3);
            }
        }

        LOG.debug("Input buffer: {}", (Object) inputBuffer);

        for (int i = 0; i < dimension; i++) {
            for (int j = 0; j < bits; j++) {
                items.clear();
                for (int k = 0; k < j - 1; k++) {
                    items.add(inputBuffer[i][k]);
                }
                for (int l = 0; l < bits; l++) {
                    if (l == j) continue;
                    for (int k = 0; k < i; k++) {
                        items.add(inputBuffer[k][l]);
                    }
                }
                resultBuffer[i * bits + j] = t(inputBuffer[i][j], items);
            }
        }

        LOG.debug("Result buffer: {}", (Object) resultBuffer);

        BigInteger result = BigInteger.ZERO;
        for (int i = 0; i < dimension * bits; i++) {
            result =
                    result.multiply(BigInteger.valueOf(3)).add(BigInteger.valueOf(resultBuffer[i]));
        }
        return result;
    }

    private int t(int mu, List<Integer> items) {
        int sum = 0;
        for (int item : items) {
            sum += item;
        }
        if (sum % 2 == 0) {
            return mu;
        } else {
            return 2 - mu;
        }
    }

    public static class Builder implements SpaceFillingCurve.Builder {

        private int dimension;
        private int bits;

        @Override
        public Builder setDimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        @Override
        public Builder setBits(int bits) {
            this.bits = bits;
            return this;
        }

        @Override
        public int getBits() {
            return bits;
        }

        @Override
        public int getDimension() {
            return dimension;
        }

        @Override
        public PeanoCurve build() {
            return new PeanoCurve(this);
        }
    }
}
