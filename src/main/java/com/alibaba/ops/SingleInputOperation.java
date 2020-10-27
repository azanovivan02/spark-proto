package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public interface SingleInputOperation {

    void apply(Row inputRow, OutputCollector collector);
}
