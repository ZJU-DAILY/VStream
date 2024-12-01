package cn.edu.zju.daily.partitioner.curve;

import java.math.BigInteger;

public class GrayCurve implements SpaceFillingCurve {

    private final ZOrderCurve zOrderCurve;
    private final long[] buffer;
    private final int dim;

    private GrayCurve(SpaceFillingCurve.Builder builder) {
        this.zOrderCurve =
                new ZOrderCurve.Builder()
                        .setDimension(builder.getDimension())
                        .setBits(builder.getBits())
                        .build();
        this.buffer = new long[builder.getDimension()];
        this.dim = builder.getDimension();
    }

    @Override
    public BigInteger index(long[] vector) {
        if (vector.length != this.dim) {
            throw new IllegalArgumentException("Vector dimension mismatch.");
        }
        for (int i = 0; i < vector.length; i++) {
            buffer[i] = vector[i] ^ (vector[i] >> 1);
        }
        return zOrderCurve.index(buffer);
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
        public GrayCurve build() {
            return new GrayCurve(this);
        }
    }
}
