package com.alibaba.ui;

import com.alibaba.nodes.CompNode;
import com.alibaba.nodes.Connection;
import com.alibaba.ops.Operation;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.view.Viewer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GraphVisualizer {

    public static void visualizeGraph(List<CompNode> startCompNodes) {
        System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        System.setProperty("org.graphstream.ui", "swing");

        Graph visualGraph = new SingleGraph("Main");
//        visualGraph.setAttribute("ui.stylesheet", "url('/Users/ivan.azanov/Documents/Spark/spark-proto/src/main/resources/style.css')");
        visualGraph.setAttribute("ui.stylesheet", STYLESHEET);

        Map<CompNode, Node> compVisualNodeMapping = new LinkedHashMap<>();
        for (CompNode compNode : startCompNodes) {
            visit(compNode, visualGraph, null, compVisualNodeMapping);
        }

        Viewer viewer = visualGraph.display();
    }

    private static void visit(CompNode currentCompNode, Graph visualGraph, Node previousVisualNode, Map<CompNode, Node> compVisualNodeMapping) {
        if (compVisualNodeMapping.containsKey(currentCompNode)) {
            Node currentVisualNode = compVisualNodeMapping.get(currentCompNode);
            addEdgeToPreviousNodeIfNeeded(visualGraph, previousVisualNode, currentVisualNode);
            return;
        }

        Node currentVisualNode = createVisualNode(currentCompNode, visualGraph);
        compVisualNodeMapping.put(currentCompNode, currentVisualNode);

        addEdgeToPreviousNodeIfNeeded(visualGraph, previousVisualNode, currentVisualNode);
        addCssClasses(currentCompNode, previousVisualNode, currentVisualNode);

        for (Connection info : currentCompNode.getNextNodes()) {
            visit(info.getNode(), visualGraph, currentVisualNode, compVisualNodeMapping);
        }
    }

    private static Node createVisualNode(CompNode currentCompNode, Graph visualGraph) {
        Operation operation = currentCompNode.getOperation();
        if (operation == null) {
            throw new IllegalStateException("No operation");
        }
        String nodeLabel = operation.getClass().getSimpleName();
        UUID nodeId = UUID.randomUUID();
        Node currentVisualNode = visualGraph.addNode(nodeLabel + nodeId);
        currentVisualNode.setAttribute("ui.label", nodeLabel);
        return currentVisualNode;
    }

    private static void addEdgeToPreviousNodeIfNeeded(Graph visualGraph, Node previousVisualNode, Node currentVisualNode) {
        if (previousVisualNode != null) {
            String edgeId = UUID.randomUUID().toString();
            visualGraph.addEdge(edgeId, previousVisualNode, currentVisualNode, true);
        }
    }

    private static void addCssClasses(CompNode currentCompNode, Node previousVisualNode, Node currentVisualNode) {
        if (previousVisualNode == null) {
            currentVisualNode.setAttribute("ui.class", "input");
        }
        if (currentCompNode.getNextNodes().isEmpty()) {
            currentVisualNode.setAttribute("ui.class", "output");
        }
    }

    public static final String STYLESHEET = "graph {\n" +
            "    padding: 100px, 100px;\n" +
            "}\n" +
            "\n" +
            "node {\n" +
            "    size: 15px;\n" +
            "    fill-color: black;\n" +
            "    text-alignment: at-right;\n" +
            "    text-offset: 30, 30;\n" +
            "    text-size: 30;\n" +
            "    text-background-mode: plain;\n" +
            "}\n" +
            "\n" +
            "node.input {\n" +
            "    fill-color: green;\n" +
            "}\n" +
            "\n" +
            "node.output {\n" +
            "    fill-color: red;\n" +
            "}\n" +
            "\n" +
            "edge {\n" +
            "    arrow-shape: arrow;\n" +
            "    arrow-size: 20px, 4px;\n" +
            "}";
}
