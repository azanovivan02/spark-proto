package com.alibaba;

import com.alibaba.nodes.NodeGateInfo;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.Operation;

public class GraphBuilder {

    public static GraphBuilder startWith(SparkNode node) {
        GraphBuilder graphBuilder = new GraphBuilder();
        graphBuilder.startNode = node;
        graphBuilder.endNode = node;
        return graphBuilder;
    }

    public static GraphBuilder startWith(Operation operation) {
        return startWith(new SparkNode(operation));
    }

    private SparkNode startNode;
    private SparkNode endNode;

    private GraphBuilder() {
    }

    private GraphBuilder(SparkNode startNode, SparkNode endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public SparkNode getStartNode() {
        return startNode;
    }

    public SparkNode getEndNode() {
        return endNode;
    }

    public GraphBuilder then(SparkNode newNode) {
        endNode.getNextNodes().add(new NodeGateInfo(newNode, 0));
        endNode = newNode;
        return this;
    }

    public GraphBuilder then(Operation operation) {
        SparkNode newNode = new SparkNode(operation);
        return then(newNode);
    }

    public GraphBuilder branch() {
        return new GraphBuilder(startNode, endNode);
    }

    public GraphBuilder join(GraphBuilder rightGraphBuilder, Operation joinOperation) {
        SparkNode newNode = new SparkNode(joinOperation);
        this.endNode.getNextNodes().add(new NodeGateInfo(newNode, 0));
        rightGraphBuilder.endNode.getNextNodes().add(new NodeGateInfo(newNode, 1));

        this.endNode = newNode;
        return this;
    }
}
