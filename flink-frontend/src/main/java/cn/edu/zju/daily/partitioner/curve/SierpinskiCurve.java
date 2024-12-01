package cn.edu.zju.daily.partitioner.curve;

import java.math.BigInteger;

public class SierpinskiCurve implements SpaceFillingCurve {

    private final int dimension;
    private final int bits;

    private SierpinskiCurve(SpaceFillingCurve.Builder builder) {
        this.dimension = builder.getDimension();
        this.bits = builder.getBits();
    }

    @Override
    public BigInteger index(long[] vector) {
        throw new UnsupportedOperationException("To be implemented");
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
        public SierpinskiCurve build() {
            return new SierpinskiCurve(this);
        }
    }
}
