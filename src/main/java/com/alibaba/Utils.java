package com.alibaba;

import com.alibaba.nodes.InputProcessor;
import com.alibaba.nodes.SingleInputNode;
import com.alibaba.ops.single.SingleInputOperation;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static List<Row> convertToRows(Collection<?> inputValues) {
        return inputValues
                .stream()
                .map(values -> new Row(Arrays.asList(values)))
                .collect(Collectors.toList());
    }

    public static SingleInputNode chainOperations(SingleInputOperation...operations) {
        if (operations.length == 0) {
            return null;
        }

        SingleInputNode firstNode = new SingleInputNode(operations[0]);
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
