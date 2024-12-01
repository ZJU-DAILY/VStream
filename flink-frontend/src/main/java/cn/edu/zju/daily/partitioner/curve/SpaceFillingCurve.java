package cn.edu.zju.daily.partitioner.curve;

import java.io.Serializable;
import java.math.BigInteger;

public interface SpaceFillingCurve {

    BigInteger index(long[] vector);

    interface Builder extends Serializable {

        /** Dimension of the vector that the space-filling curve is applied to. */
        Builder setDimension(int dimension);

        /** Number of bits to represent each dimension. */
        Builder setBits(int bits);

        int getDimension();

        int getBits();

        SpaceFillingCurve build();
    }
}
