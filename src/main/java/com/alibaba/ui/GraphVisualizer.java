package com.alibaba.ui;

import com.alibaba.nodes.CompNode;
import com.alibaba.nodes.Connection;
import com.alibaba.ops.Operator.OpType;
import org.graphstream.graph.Element;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.layout.HierarchicalLayout;
import org.graphstream.ui.swing.SwingGraphRenderer;
import org.graphstream.ui.swing_viewer.SwingViewer;
import org.graphstream.ui.view.GraphRenderer;
import org.graphstream.ui.view.Viewer.ThreadingModel;

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

//        visualGraph.setAttribute("layout.force", 3);
//        visualGraph.setAttribute("layout.quality", 4);

        Map<CompNode, Node> compVisualNodeMapping = new LinkedHashMap<>();
        for (CompNode compNode : startCompNodes) {
            visit(compNode, visualGraph, null, compVisualNodeMapping);
        }

        SwingViewer viewer = new SwingViewer(visualGraph, ThreadingModel.GRAPH_IN_ANOTHER_THREAD);
        viewer.enableAutoLayout();
        GraphRenderer renderer = new SwingGraphRenderer();
        viewer.addView(SwingViewer.DEFAULT_VIEW_ID, renderer);
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

        for (Connection info : currentCompNode.getConnections()) {
            visit(info.getNode(), visualGraph, currentVisualNode, compVisualNodeMapping);
        }
    }

    private static Node createVisualNode(CompNode currentCompNode, Graph visualGraph) {
        Object operator = currentCompNode.getOperator();
        if (operator == null) {
            throw new IllegalStateException("No operator");
        }
        String nodeLabel = operator.getClass().getSimpleName();
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
//            currentVisualNode.setAttribute("layout.weight", 5);
            return;
        }
        if (currentCompNode.getConnections().isEmpty()) {
            currentVisualNode.setAttribute("ui.class", "output");
            return;
        }
        OpType opType = currentCompNode.getOpType();
        switch (opType) {
            case REDUCER: {
                currentVisualNode.setAttribute("ui.class", "reducer");
                break;
            }
            case JOINER: {
                currentVisualNode.setAttribute("ui.class", "joiner");
                break;
            }
        }
    }

    private static HierarchicalLayout createLayout(List<CompNode> startCompNodes, Map<CompNode, Node> compVisualNodeMapping) {
//        HierarchicalLayout layout = new HierarchicalLayout();
        String[] rootIds =
                startCompNodes
                        .stream()
                        .map(compVisualNodeMapping::get)
                        .map(Element::getId).toArray(String[]::new);
//        layout.setRoots(rootIds);
//        layout.setQuality(4);
//        layout.setForce(10);
//        return layout;

        return new HierarchicalLayout();
    }

    public static final String STYLESHEET = "graph {\n" +
            "    padding: 100px, 100px;\n" +
            "}\n" +
            "\n" +
            "node {\n" +
            "    size: 15px;\n" +
            "    text-alignment: at-right;\n" +
            "    text-offset: 30, 30;\n" +
            "    text-size: 30;\n" +
            "    text-background-mode: plain;\n" +
            "    fill-color: #DEE;\n" +
            "    size: 15px;\n" +
            "    stroke-mode: plain;\n" +
            "    stroke-color: #555;\n" +
            "}\n" +
            "\n" +
            "\n" +
            "node.input {\n" +
            "    fill-color: green;\n" +
            "}\n" +
            "\n" +
            "node.output {\n" +
            "    fill-color: red;\n" +
            "}\n" +
            "\n" +
            "node.reducer {\n" +
            "    fill-color: blue;\n" +
            "}\n" +
            "\n" +
            "node.joiner {\n" +
            "    fill-color: orange;\n" +
            "}\n" +
            "\n" +
            "edge {\n" +
            "    arrow-shape: arrow;\n" +
            "    arrow-size: 20px, 4px;\n" +
            "}";
}
