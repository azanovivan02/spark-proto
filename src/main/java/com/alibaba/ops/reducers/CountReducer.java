package com.alibaba.ops.reducers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.OpUtils;
import com.alibaba.ops.Operator;

public class CountReducer implements Operator.Reducer {

    private String outputCountColumn;
    private final String[] groupByColumns;

    Row currentRow = null;
    int currentCount = 0;

    public CountReducer(String outputCountColumn, String... groupByColumns) {
        this.outputCountColumn = outputCountColumn;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (inputRow.isTerminal()) {
            if (currentRow != null) {
                outputCountRow(collector);
            }

            currentRow = null;
            collector.collect(inputRow);
            return;
        }

        if (currentRow == null || !OpUtils.equalByColumns(inputRow, currentRow, groupByColumns)) {
            if (currentRow != null) {
                outputCountRow(collector);
            }
            currentCount = 1;
            currentRow = inputRow;
        } else {
            currentCount++;
        }
    }

    private void outputCountRow(OutputCollector collector) {
        Row newRow = currentRow.copyColumns(groupByColumns);
        newRow.set(outputCountColumn, currentCount);
        collector.collect(newRow);
    }

}
