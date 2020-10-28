package com.alibaba.ops.mappers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator;

import java.util.function.Function;

public class LambdaMapper<I> implements Operator.Mapper {

    private final Function<I, ?> lambda;
    private String column;

    public LambdaMapper(String column, Function<I, ?> lambda) {
        this.lambda = lambda;
        this.column = column;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        I inputValue = (I) inputRow.get(column);
        Object outputValue = lambda.apply(inputValue);

        Row outputRow = inputRow
                .copy()
                .set(column, outputValue);
        collector.collect(outputRow);
    }
}
