package com.alibaba.ui;

import com.alibaba.nodes.NodeGateInfo;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.Operation;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.view.Viewer;

import java.util.UUID;

public class GraphVisualizer {

    public static void visualizeGraph(SparkNode baseCaseGraph) {
        System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        System.setProperty("org.graphstream.ui", "swing");

        Graph visualGraph = new SingleGraph("Main");
        visualGraph.setAttribute("ui.stylesheet", "url('/Users/ivan.azanov/Documents/Spark/spark-proto/src/main/resources/style.css')");

        visit(baseCaseGraph, visualGraph, null);
        Viewer viewer = visualGraph.display();
    }

    private static void visit(SparkNode node, Graph visualGraph, Node previousVisualNode) {
        Operation operation = node.getOperation();

        if (operation == null) {
            throw new IllegalStateException("No operation");
        }

        String nodeLabel = operation.getClass().getSimpleName();
        UUID nodeId = UUID.randomUUID();

        Node currentVisualNode = visualGraph.addNode(nodeLabel + nodeId);
        currentVisualNode.setAttribute("ui.label", nodeLabel);

        if (previousVisualNode != null) {
            String edgeId = UUID.randomUUID().toString();
            visualGraph.addEdge(edgeId, previousVisualNode, currentVisualNode, true);
        } else {
            currentVisualNode.setAttribute("ui.class", "input");
        }

        System.out.println(nodeLabel);

        if (previousVisualNode == null) {
            currentVisualNode.setAttribute("ui.class", "input");
        }
        if (node.getNextNodes().isEmpty()) {
            currentVisualNode.setAttribute("ui.class", "output");
        }

        for (NodeGateInfo info : node.getNextNodes()) {
            visit(info.getNode(), visualGraph, currentVisualNode);
        }
    }
}
