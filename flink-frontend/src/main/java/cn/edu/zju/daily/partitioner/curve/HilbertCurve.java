package cn.edu.zju.daily.partitioner.curve;

import java.math.BigInteger;

public class HilbertCurve implements SpaceFillingCurve {

    private final org.davidmoten.hilbert.HilbertCurve proxy;

    private HilbertCurve(org.davidmoten.hilbert.HilbertCurve proxy) {
        this.proxy = proxy;
    }

    @Override
    public BigInteger index(long[] vector) {
        return proxy.index(vector);
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
        public HilbertCurve build() {
            org.davidmoten.hilbert.HilbertCurve proxy =
                    org.davidmoten.hilbert.HilbertCurve.bits(bits).dimensions(dimension);
            return new HilbertCurve(proxy);
        }
    }
}
