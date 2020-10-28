package com.alibaba.ops.mappers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator;

public class Printer implements Operator.Mapper {

    private final String prefix;

    public Printer(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        System.out.printf("%s: %s%n", prefix, inputRow);
        collector.collect(inputRow);
    }
}
