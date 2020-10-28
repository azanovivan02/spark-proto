package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public class Print implements SingleInputOperation {

    private final String prefix;

    public Print(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        System.out.printf("%s: %s%n", prefix, inputRow);
        collector.push(inputRow);
    }
}
