package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.io.Serializable;

public interface LSHash extends Serializable {
    int hash(FloatVector vector);
}
