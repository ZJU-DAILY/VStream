package cn.edu.zju.daily.partitioner;

import cn.edu.zju.daily.data.vector.FloatVector;
import java.util.List;

public interface Initializable {

    void initialize(List<FloatVector> vectors);
}
