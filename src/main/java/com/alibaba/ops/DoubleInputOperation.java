package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.single.SingleInputOperation;

public interface DoubleInputOperation extends SingleInputOperation  {

    void applyLeft(Row inputRow, OutputCollector collector);
    void applyRight(Row inputRow, OutputCollector collector);
}
