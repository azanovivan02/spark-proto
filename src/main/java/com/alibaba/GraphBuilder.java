package com.alibaba;

import com.alibaba.nodes.Connection;
import com.alibaba.nodes.Node;
import com.alibaba.ops.Operation;

import java.util.List;

public class GraphBuilder {

    public static GraphBuilder startWith(Node node) {
        GraphBuilder graphBuilder = new GraphBuilder();
        graphBuilder.startNode = node;
        graphBuilder.endNode = node;
        return graphBuilder;
    }

    public static GraphBuilder startWith(Operation operation) {
        return startWith(new Node(operation));
    }

    private Node startNode;
    private Node endNode;

    private GraphBuilder() {
    }

    private GraphBuilder(Node startNode, Node endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public Node getStartNode() {
        return startNode;
    }

    public Node getEndNode() {
        return endNode;
    }

    public GraphBuilder then(Node newNode) {
        endNode.addConnection(newNode, 0);
        endNode = newNode;
        return this;
    }

    public GraphBuilder then(Operation operation) {
        Node newNode = new Node(operation);
        return then(newNode);
    }

    public GraphBuilder branch() {
        return new GraphBuilder(startNode, endNode);
    }

    public GraphBuilder join(GraphBuilder rightGraphBuilder, Operation joinOperation) {
        Node joinNode = new Node(joinOperation);
        Node leftInputNode = this.endNode;
        Node rightInputNode = rightGraphBuilder.endNode;

        leftInputNode.addConnection(joinNode, 0);
        rightInputNode.addConnection(joinNode, 1);
        this.endNode = joinNode;

        return this;
    }
}
