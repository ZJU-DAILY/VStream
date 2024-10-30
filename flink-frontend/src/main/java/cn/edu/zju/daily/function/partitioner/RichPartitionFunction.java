package cn.edu.zju.daily.function.partitioner;

import org.apache.flink.api.common.functions.AbstractRichFunction;

public abstract class RichPartitionFunction extends AbstractRichFunction
        implements PartitionFunction {}
