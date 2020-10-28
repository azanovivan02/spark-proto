package com.alibaba.ops.mappers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator.Mapper;

public class RetainColumnsMapper implements Mapper {

    private final String[] retainedColumns;

    public RetainColumnsMapper(String...retainedColumns) {
        this.retainedColumns = retainedColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        Row newRow = inputRow.copyColumns(retainedColumns);
        collector.collect(newRow);
    }
}
