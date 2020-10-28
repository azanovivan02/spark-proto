package com.alibaba.ops.mappers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator;

import java.util.function.Function;

public class AddColumnMapper implements Operator.Mapper {

    private final Function<Row, ?> lambda;
    private String outputColumn;

    public AddColumnMapper(String outputColumn, Function<Row, ?> lambda) {
        this.lambda = lambda;
        this.outputColumn = outputColumn;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        Object outputValue = lambda.apply(inputRow);

        Row outputRow = inputRow
                .copy()
                .set(outputColumn, outputValue);
        collector.collect(outputRow);
    }
}
