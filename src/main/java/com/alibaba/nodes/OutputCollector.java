package com.alibaba.nodes;

import com.alibaba.Row;

public interface OutputCollector {
    void push(Row outputRow);
}
