package cn.edu.zju.daily.data.source.rate;

import java.io.Serializable;

public interface WaitingIndexBuildStrategy extends Serializable {

    void waitIndexBuild(float waitRatio);
}
