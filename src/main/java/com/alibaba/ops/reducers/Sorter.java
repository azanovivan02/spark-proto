package com.alibaba.ops.reducers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.OpUtils;
import com.alibaba.ops.Operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.sort;

public class Sorter implements Operator.Reducer {

    private final List<Row> accumulatedRows = new ArrayList<>();

    private final Order order;
    private final String[] keyColumns;
    private final Comparator<Row> rowComparator;

    public Sorter(Order order, String... keyColumns) {
        this.order = order;
        this.keyColumns = keyColumns;

        Comparator<Row> comparator = (o1, o2) -> OpUtils.compareRows(o1, o2, keyColumns);

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
