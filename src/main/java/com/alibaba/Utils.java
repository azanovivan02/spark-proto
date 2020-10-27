package com.alibaba;

import com.alibaba.nodes.InputProcessor;
import com.alibaba.nodes.SingleInputNode;
import com.alibaba.ops.single.SingleInputOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

    public static SingleInputNode chainOperations(SingleInputOperation...operations) {
        switch (operations.length) {
            case 0: {
                return null;
            }
            case 1: {
                return new SingleInputNode(operations[0]);
            }
        }

        SingleInputNode firstNode = new SingleInputNode(operations[0]);
        SingleInputNode previousNode = firstNode;
        for (int operationIndex = 1; operationIndex < operations.length; operationIndex++) {
            SingleInputOperation currentOperation = operations[operationIndex];
            SingleInputNode currentNode = new SingleInputNode(currentOperation);
            previousNode
                    .getNextNodes()
                    .add(currentNode);
            previousNode = currentNode;
        }

        return firstNode;
    }

    public static SingleInputNode appendOperations(SingleInputNode node, SingleInputOperation...operations) {
        if (operations.length == 0) {
            return node;
        }

        SingleInputNode firstNode = node;
        SingleInputNode previousNode = firstNode;
        for (int operationIndex = 0; operationIndex < operations.length; operationIndex++) {
            SingleInputOperation currentOperation = operations[operationIndex];
            SingleInputNode currentNode = new SingleInputNode(currentOperation);
            previousNode
                    .getNextNodes()
                    .add(currentNode);
            previousNode = currentNode;
        }

        return firstNode;
    }

    public static SingleInputNode getLastNode(SingleInputNode firstNode) {
        if (firstNode == null) {
            return null;
        }

        SingleInputNode currentNode = firstNode;
        while (true) {
            List<InputProcessor> nextNodes = currentNode.getNextNodes();
            switch (nextNodes.size()) {
                case 0: {
                    return currentNode;
                }
                case 1: {
                    currentNode = (SingleInputNode) nextNodes.get(0);
                    break;
                }
                default: {
                    throw new IllegalStateException("Only single next node is accepted, but got: "+nextNodes);
                }
            }
        }
    }
}
