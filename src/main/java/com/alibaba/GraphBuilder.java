package com.alibaba;

import com.alibaba.nodes.CompNode;
import com.alibaba.ops.Operation;

public class GraphBuilder {

    public static GraphBuilder startWith(CompNode node) {
        GraphBuilder graphBuilder = new GraphBuilder();
        graphBuilder.startNode = node;
        graphBuilder.endNode = node;
        return graphBuilder;
    }

    public static GraphBuilder startWith(Operation operation) {
        return startWith(new CompNode(operation));
    }

    private CompNode startNode;
    private CompNode endNode;

    private GraphBuilder() {
    }

    private GraphBuilder(CompNode startNode, CompNode endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public CompNode getStartNode() {
        return startNode;
    }

    public CompNode getEndNode() {
        return endNode;
    }

    public GraphBuilder then(CompNode newNode) {
        endNode.addConnection(newNode, 0);
        endNode = newNode;
        return this;
    }

    public GraphBuilder then(Operation operation) {
        CompNode newNode = new CompNode(operation);
        return then(newNode);
    }

    public GraphBuilder branch() {
        return new GraphBuilder(startNode, endNode);
    }

    public GraphBuilder join(GraphBuilder rightGraphBuilder, Operation joinOperation) {
        CompNode joinNode = new CompNode(joinOperation);
        CompNode leftInputNode = this.endNode;
        CompNode rightInputNode = rightGraphBuilder.endNode;

        leftInputNode.addConnection(joinNode, 0);
        rightInputNode.addConnection(joinNode, 1);
        this.endNode = joinNode;

        return this;
    }
}
