package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

import java.util.function.Function;

public class LambdaMap<I, O> implements SingleInputOperation {

    private final Function<I, O> lambda;
    private String column;

    public LambdaMap(String column, Function<I, O> lambda) {
        this.lambda = lambda;
        this.column = column;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        I inputValue = (I) inputRow.get(column);
        O outputValue = lambda.apply(inputValue);

        Row outputRow = inputRow
                .copy()
                .set(column, outputValue);
        collector.collect(outputRow);
    }
}
