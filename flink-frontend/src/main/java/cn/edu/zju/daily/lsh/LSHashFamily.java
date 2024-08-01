package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.FloatVector;

import java.io.Serializable;

public interface LSHashFamily extends Serializable {

    int[] hash(FloatVector vector);
    int getNumHashFunctions();
}
