package com.alibaba.nodes;

public class NodeGateInfo {
    private final SparkNode node;
    private final int gateNumber;

    public NodeGateInfo(SparkNode node, int gateNumber) {
        this.node = node;
        this.gateNumber = gateNumber;
    }

    public SparkNode getNode() {
        return node;
    }

    public int getGateNumber() {
        return gateNumber;
    }
}
