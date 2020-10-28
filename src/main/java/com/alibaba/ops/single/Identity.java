package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.Operation;

public class Identity implements Operation {

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        collector.collect(inputRow);
    }

}
