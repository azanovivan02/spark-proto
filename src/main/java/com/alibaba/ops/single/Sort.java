package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.TerminalAwareOperation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.sort;

public class Sort implements TerminalAwareOperation {

    private final List<Row> accumulatedRows = new ArrayList<>();

    private final Order order;
    private final String[] keyColumns;
    private final Comparator<Row> rowComparator;

    public Sort(Order order, String... keyColumns) {
        this.order = order;
        this.keyColumns = keyColumns;

        Comparator<Row> comparator = new Comparator<Row>() {
            @Override
            public int compare(Row o1, Row o2) {
                for (String column : keyColumns) {
                    Comparable leftValue = o1.getComparable(column);
                    Comparable rightValue = o2.getComparable(column);
                    int comparisonResult = leftValue.compareTo(rightValue);
                    if (comparisonResult != 0) {
                        return comparisonResult;
                    }
                }

                return 0;
            }

        };

        if (order == Order.DESCENDING) {
            this.rowComparator = comparator.reversed();
        } else {
            this.rowComparator = comparator;
        }
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (!inputRow.isTerminal()) {
            accumulatedRows.add(inputRow);
            return;
        }

        sort(accumulatedRows, rowComparator);
        for (Row row : accumulatedRows) {
            collector.collect(row);
        }
        collector.collect(Row.terminalRow());
    }

    public enum Order {
        ASCENDING,
        DESCENDING
    }
}
