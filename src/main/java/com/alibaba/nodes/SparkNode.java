package com.alibaba.nodes;

import com.alibaba.Row;
import com.alibaba.ops.DoubleInputOperation;
import com.alibaba.ops.OpType;
import com.alibaba.ops.single.SingleInputOperation;
import com.alibaba.ops.single.TerminalAwareOperation;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.ops.OpType.DOUBLE_INPUT;
import static com.alibaba.ops.OpType.SINGLE_INPUT;
import static com.alibaba.ops.OpType.TERMINAL_AWARE;

public class SparkNode {

    private final SingleInputOperation operation;
    private final OpType opType;
    private final List<NodeGateInfo> nextNodes;

    public SparkNode(SingleInputOperation operation) {
        this.operation = operation;
        this.nextNodes = new ArrayList<>();
        this.opType = getOpType(operation);
    }

    private static OpType getOpType(SingleInputOperation operation) {
        if (operation instanceof DoubleInputOperation) {
            return DOUBLE_INPUT;
        } else if (operation instanceof TerminalAwareOperation) {
            return TERMINAL_AWARE;
        } else {
            return SINGLE_INPUT;
        }
    }

    public SingleInputOperation getOperation() {
        return operation;
    }

    public List<NodeGateInfo> getNextNodes() {
        return nextNodes;
    }

    public void push(Row inputRow, int gateNumber) {
        switch (opType) {
            case SINGLE_INPUT: {
                if (!inputRow.isTerminal()) {
                    operation.apply(inputRow, this::collect);
                } else {
                    collect(inputRow);
                }
                break;
            }
            case TERMINAL_AWARE: {
                operation.apply(inputRow, this::collect);
                break;
            }
            case DOUBLE_INPUT: {
                DoubleInputOperation doubleInputOperation = (DoubleInputOperation) this.operation;
                switch (gateNumber) {
                    case 0: {
                        doubleInputOperation.applyLeft(inputRow, this::collect);
                        break;
                    }
                    case 1: {
                        doubleInputOperation.applyRight(inputRow, this::collect);
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unknown gate");
                    }
                }
            }
        }
    }

    private void collect(Row row) {
        for (NodeGateInfo info : nextNodes) {
            SparkNode node = info.getNode();
            int gateNumber = info.getGateNumber();
            node.push(row, gateNumber);
        }
    }
}
