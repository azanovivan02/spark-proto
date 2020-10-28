package com.alibaba.nodes;

public class Connection {
    private final Node node;
    private final int gate;

    public Connection(Node node, int gate) {
        this.node = node;
        this.gate = gate;
    }

    public Node getNode() {
        return node;
    }

    public int getGate() {
        return gate;
    }
}
