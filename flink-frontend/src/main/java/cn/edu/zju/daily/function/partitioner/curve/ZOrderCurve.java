package cn.edu.zju.daily.function.partitioner.curve;

import java.math.BigInteger;

public class ZOrderCurve implements SpaceFillingCurve {

    private final int dimension;
    private final int bits;

    private ZOrderCurve(SpaceFillingCurve.Builder builder) {
        this.dimension = builder.getDimension();
        this.bits = builder.getBits();
    }

    @Override
    public BigInteger index(long[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException("Vector dimension mismatch.");
        }

        BigInteger result = BigInteger.ZERO;
        for (int i = bits - 1; i >= 0; i--) {
            for (int j = 0; j < dimension; j++) {
                long bit = (vector[j] >> i) & 1L;
                BigInteger shifted = BigInteger.valueOf(bit).shiftLeft(dimension * i + j);
                result = result.or(shifted);
            }
        }
        return result;
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
            return null;
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
        public ZOrderCurve build() {
            return new ZOrderCurve(this);
        }
    }
}
