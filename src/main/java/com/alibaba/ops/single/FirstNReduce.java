package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public class FirstNReduce implements SingleInputOperation {

    private final int maxAmount;
    private int currentAmount = 0;

    public FirstNReduce(int maxAmount) {
        this.maxAmount = maxAmount;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (currentAmount < maxAmount) {
            collector.collect(inputRow);
        } else if (currentAmount == maxAmount) {
            collector.collect(Row.terminalRow());
        }
        currentAmount++;
    }
}
