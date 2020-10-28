package com.alibaba.ops;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;

import java.util.LinkedList;

import static com.alibaba.ops.OpUtils.compareRows;

public class InnerJoin implements Operator.Joiner {

    LinkedList<Row> leftRows = new LinkedList<>();
    LinkedList<Row> rightRows = new LinkedList<>();

    private final String[] keyColumns;

    public InnerJoin(String... keyColumns) {
        this.keyColumns = keyColumns;
    }

    @Override
    public void applyLeft(Row inputRow, OutputCollector collector) {
        leftRows.add(inputRow);
        outputJoinedRowsIfPossible(collector);
    }

    @Override
    public void applyRight(Row inputRow, OutputCollector collector) {
        rightRows.add(inputRow);
        outputJoinedRowsIfPossible(collector);
    }

    private void outputJoinedRowsIfPossible(OutputCollector collector) {
        if (leftRows.isEmpty() || !leftRows.getLast().isTerminal()) {
            return;
        }
        if (rightRows.isEmpty() || !rightRows.getLast().isTerminal()) {
            return;
        }

        leftRows.removeLast();
        rightRows.removeLast();

        for (Row rightRow : rightRows) {
            for (Row leftRow : leftRows) {
                int comparisonResult = compareRows(leftRow, rightRow, keyColumns);
                if (comparisonResult == 0) {
                    Row joinedRow = leftRow.copy().setAll(rightRow.getValues());
                    collector.collect(joinedRow);
                }
            }
        }
        collector.collect(Row.terminalRow());
    }

}
