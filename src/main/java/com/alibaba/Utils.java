package com.alibaba;

import com.alibaba.nodes.NodeGateInfo;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.single.SingleInputOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

public class Utils {

    public static List<Row> convertToRows(String columnName, Collection<?> inputValues) {
        return inputValues
                .stream()
                .map(value -> new Row(singletonMap(columnName, value)))
                .collect(Collectors.toList());
    }

    public static List<Row> convertToRows(String[] schema, Object[]...inputTuples) {
        ArrayList<Row> outputRows = new ArrayList<>();
        for (Object[] tuple : inputTuples) {
            Row row = new Row(new HashMap<>());
            for (int columnIndex = 0; columnIndex < schema.length; columnIndex++) {
                String columnName = schema[columnIndex];
                Object columnValue = tuple[columnIndex];
                row.set(columnName, columnValue);
            }
            outputRows.add(row);
        }

        return outputRows;
    }

    public static SparkNode chainOperations(SingleInputOperation...operations) {
        switch (operations.length) {
            case 0: {
                return null;
            }
            case 1: {
                return new SparkNode(operations[0]);
            }
        }

        SparkNode firstNode = new SparkNode(operations[0]);
        SparkNode previousNode = firstNode;
        for (int operationIndex = 1; operationIndex < operations.length; operationIndex++) {
            SingleInputOperation currentOperation = operations[operationIndex];
            SparkNode currentNode = new SparkNode(currentOperation);
            previousNode
                    .getNextNodes()
                    .add(new NodeGateInfo(currentNode, 0));
            previousNode = currentNode;
        }

        return firstNode;
    }

    public static SparkNode appendOperations(SparkNode node, SingleInputOperation...operations) {
        if (operations.length == 0) {
            return node;
        }

        SparkNode firstNode = node;
        SparkNode previousNode = firstNode;
        for (int operationIndex = 0; operationIndex < operations.length; operationIndex++) {
            SingleInputOperation currentOperation = operations[operationIndex];
            SparkNode currentNode = new SparkNode(currentOperation);
            previousNode
                    .getNextNodes()
                    .add(new NodeGateInfo(currentNode, 0));
            previousNode = currentNode;
        }

        return firstNode;
    }

    public static SparkNode getLastNode(SparkNode firstNode) {
        if (firstNode == null) {
            return null;
        }

        SparkNode currentNode = firstNode;
        while (true) {
            List<NodeGateInfo> nextNodes = currentNode.getNextNodes();
            switch (nextNodes.size()) {
                case 0: {
                    return currentNode;
                }
                case 1: {
                    currentNode = nextNodes.get(0).getNode();
                    break;
                }
                default: {
                    throw new IllegalStateException("Only single next node is accepted, but got: "+nextNodes);
                }
            }
        }
    }
}
