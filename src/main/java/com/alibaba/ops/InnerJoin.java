package com.alibaba.ops;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;

import java.util.LinkedList;

public class InnerJoin implements DoubleInputOperation {

    LinkedList<Row> leftRows = new LinkedList<>();
    LinkedList<Row> rightRows = new LinkedList<>();

    private final String keyColumn;

    public InnerJoin(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        throw new IllegalStateException("Not implemented");
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
                int comparisonResult = compareRows(leftRow, rightRow, keyColumn);
                if (comparisonResult == 0) {
                    Row joinedRow = leftRow.copy().setAll(rightRow.getValues());
                    collector.push(joinedRow);
                }
            }
        }
    }

    private int compareRows(Row leftRow, Row rightRow, String keyColumn) {
        Comparable leftValue = leftRow.getComparable(keyColumn);
        Comparable rightValue = rightRow.getComparable(keyColumn);
        return leftValue.compareTo(rightValue);
    }
}
