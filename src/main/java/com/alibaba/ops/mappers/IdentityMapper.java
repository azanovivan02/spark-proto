package com.alibaba.ops.mappers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.Operator.Mapper;

public class IdentityMapper implements Mapper {

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        collector.collect(inputRow);
    }
}
