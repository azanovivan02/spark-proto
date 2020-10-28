package com.alibaba;

import com.alibaba.nodes.CompNode;
import com.alibaba.ops.Operator;
import com.alibaba.ops.reducers.Sorter;
import com.alibaba.ops.reducers.Sorter.Order;

public class GraphBuilder {

    public static GraphBuilder startWith(CompNode node) {
        GraphBuilder graphBuilder = new GraphBuilder();
        graphBuilder.startNode = node;
        graphBuilder.endNode = node;
        return graphBuilder;
    }

    public static GraphBuilder startWith(Operator operator) {
        return startWith(new CompNode(operator));
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

    public GraphBuilder then(Operator operator) {
        CompNode newNode = new CompNode(operator);
        endNode.addConnection(newNode, 0);
        endNode = newNode;
        return this;
    }

    public GraphBuilder sortBy(Order order, String... keyColumns) {
        Sorter sorter = new Sorter(order, keyColumns);
        return then(sorter);
    }

    public GraphBuilder branch() {
        return new GraphBuilder(startNode, endNode);
    }

    public GraphBuilder join(GraphBuilder rightGraphBuilder, Operator joinOperator) {
        CompNode joinNode = new CompNode(joinOperator);
        CompNode leftInputNode = this.endNode;
        CompNode rightInputNode = rightGraphBuilder.endNode;

        leftInputNode.addConnection(joinNode, 0);
        rightInputNode.addConnection(joinNode, 1);
        this.endNode = joinNode;

        return this;
    }
}
