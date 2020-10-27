package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public interface DoubleInputOperation {

    void applyLeft(Row inputRow, OutputCollector collector);
    void applyRight(Row inputRow, OutputCollector collector);
}
