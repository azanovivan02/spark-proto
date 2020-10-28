package com.alibaba.ops;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;

public interface Operator {

    interface Mapper extends Operator {
        void apply(Row inputRow, OutputCollector collector);
    }

    interface Reducer extends Operator {
        void apply(Row inputRow, OutputCollector collector);
    }

    interface Joiner extends Operator {
        void applyLeft(Row inputRow, OutputCollector collector);
        void applyRight(Row inputRow, OutputCollector collector);
    }

    enum OpType {
        MAPPER,
        REDUCER,
        JOINER
    }
}
