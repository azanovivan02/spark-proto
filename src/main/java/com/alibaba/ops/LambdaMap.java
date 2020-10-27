package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

import java.util.function.Function;

public class LambdaMap<I, O> implements SingleInputOperation {

    private final Function<I, O> lambda;
    private int columnIndex;

    public LambdaMap(Function<I, O> lambda, int columnIndex) {
        this.lambda = lambda;
        this.columnIndex = columnIndex;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        I inputValue = (I) inputRow.get(columnIndex);
        O outputValue = lambda.apply(inputValue);

        Row outputRow = inputRow.replaceAndCopy(columnIndex, outputValue);
        collector.collect(outputRow);
    }
}
