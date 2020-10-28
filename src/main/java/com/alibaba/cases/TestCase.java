package com.alibaba.cases;

import com.alibaba.nodes.CompNode;

import java.util.List;

public interface TestCase {
    void launch();

    List<CompNode> createGraph();
}
