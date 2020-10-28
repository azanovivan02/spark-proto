package com.alibaba.cases;

import com.alibaba.nodes.Node;

public interface TestCase {
    void launch();
    default Node createGraph() {
        throw new IllegalStateException("Not implemented");
    }
}
