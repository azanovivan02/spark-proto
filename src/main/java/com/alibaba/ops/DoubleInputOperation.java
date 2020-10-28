package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public interface DoubleInputOperation extends Operation {
    void applySecond(Row inputRow, OutputCollector collector);
}
