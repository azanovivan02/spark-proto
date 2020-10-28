package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.TerminalAwareOperation;

public class Count implements TerminalAwareOperation {

    private final String[] groupByColumns;

    Row currentRow = null;
    int currentCount = 0;

    public Count(String... groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (inputRow.isTerminal()) {
            if (currentRow != null) {
                Row newRow = currentRow.copyColumns(groupByColumns);
                newRow.set("Count", currentCount);
                collector.collect(newRow);
            }

            currentRow = null;
            collector.collect(inputRow);
            return;
        }

        if (currentRow == null || !equalByColumns(inputRow, currentRow, groupByColumns)) {
            if (currentRow != null) {
                Row newRow = currentRow.copyColumns(groupByColumns);
                newRow.set("Count", currentCount);
                collector.collect(newRow);
            }
            currentCount = 1;
            currentRow = inputRow;
        } else {
            currentCount++;
        }
    }

    private boolean equalByColumns(Row left, Row right, String... comparisonColumns) {
        for (String column : comparisonColumns) {
            Object leftValue = left.get(column);
            Object rightValue = right.get(column);
            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }
}
