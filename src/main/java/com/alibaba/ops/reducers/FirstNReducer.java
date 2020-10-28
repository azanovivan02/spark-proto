package com.alibaba.ops.reducers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator;

import static com.alibaba.ops.OpUtils.equalByColumns;

public class FirstNReducer implements Operator.Reducer {

    private final int maxAmount;
    private final String[] groupByColumns;

    Row currentRow = null;
    private int currentCount = 0;

    public FirstNReducer(int maxAmount, String...groupByColumns) {
        this.maxAmount = maxAmount;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (inputRow.isTerminal()) {
            collector.collect(inputRow);
            return;
        }

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
