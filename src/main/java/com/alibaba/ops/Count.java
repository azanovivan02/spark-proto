package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

import java.util.Arrays;
import java.util.List;

public class Count implements TerminalAwareOperation {

    private final int[] groupByIndices;

    Row currentRow = null;
    int currentCount = 0;

    public Count(int... groupByIndices) {
        this.groupByIndices = groupByIndices;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (inputRow.isTerminal()) {
            if (currentRow != null) {
                Row newRow = new Row(Arrays.asList(currentRow.get(0), currentCount));
                collector.collect(newRow);
            }

            currentRow = null;
            collector.collect(inputRow);
            return;
        }

        if (currentRow == null || !equalByColumns(inputRow, currentRow, groupByIndices)) {
            if (currentRow != null) {
                Row newRow = currentRow.addAndCopy(currentCount);
                collector.collect(newRow);
            }
            currentCount = 1;
            currentRow = inputRow;
        } else {
            currentCount++;
        }
    }

    private boolean equalByColumns(Row left, Row right, int... indices) {
        for (int index : indices) {
            Object leftValue = left.get(index);
            Object rightValue = right.get(index);
            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }
}
