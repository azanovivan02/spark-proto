package com.alibaba.ops;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;

public class DumbJoin implements DoubleInputOperation {

    @Override
    public void applyLeft(Row inputRow, OutputCollector collector) {
    }

    @Override
    public void applyRight(Row inputRow, OutputCollector collector) {
    }
}
