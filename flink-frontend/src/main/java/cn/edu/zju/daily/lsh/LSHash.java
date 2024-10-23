package cn.edu.zju.daily.lsh;

import cn.edu.zju.daily.data.vector.VectorData;
import java.io.Serializable;

public interface LSHash extends Serializable {
    int hash(VectorData vector);
}
