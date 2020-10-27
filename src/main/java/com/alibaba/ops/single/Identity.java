package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public class Identity implements SingleInputOperation {

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        collector.collect(inputRow);
    }

}
