package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.sort;

public class Sort implements TerminalAwareOperation {

    private final List<Row> accumulatedRows = new ArrayList<>();

    private final Order order;
    private final int keyIndex;
    private final Comparator<Row> rowComparator;

    public Sort(Order order, int keyIndex) {
        this.order = order;
        this.keyIndex = keyIndex;

        Comparator<Row> comparator = Comparator.comparing(row -> (Comparable) row.get(keyIndex));
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
