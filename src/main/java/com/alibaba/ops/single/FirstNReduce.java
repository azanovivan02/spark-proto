package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.Operation;

import static com.alibaba.ops.OpUtils.equalByColumns;

public class FirstNReduce implements Operation {

    private final int maxAmount;
    private final String[] groupByColumns;

    Row currentRow = null;
    private int currentCount = 0;

    public FirstNReduce(int maxAmount, String...groupByColumns) {
        this.maxAmount = maxAmount;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (currentRow == null || !equalByColumns(inputRow, currentRow, groupByColumns)) {
            currentCount = 1;
            currentRow = inputRow;
        } else {
            currentCount++;
        }

        if (currentCount <= maxAmount) {
            collector.collect(inputRow);
        }
    }
}
