package com.alibaba.nodes;

import com.alibaba.Row;
import com.alibaba.ops.DoubleInputOperation;
import com.alibaba.ops.OpType;
import com.alibaba.ops.Operation;
import com.alibaba.ops.single.TerminalAwareOperation;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.ops.OpType.DOUBLE_INPUT;
import static com.alibaba.ops.OpType.SINGLE_INPUT;
import static com.alibaba.ops.OpType.TERMINAL_AWARE;

public class SparkNode {

    private final Operation operation;
    private final OpType opType;
    private final List<NodeGateInfo> nextNodes;

    public SparkNode(Operation operation) {
        this.operation = operation;
        this.nextNodes = new ArrayList<>();
        this.opType = getOpType(operation);
    }

    private static OpType getOpType(Operation operation) {
        if (operation instanceof DoubleInputOperation) {
            return DOUBLE_INPUT;
        } else if (operation instanceof TerminalAwareOperation) {
            return TERMINAL_AWARE;
        } else {
            return SINGLE_INPUT;
        }
    }

    public Operation getOperation() {
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
                        doubleInputOperation.apply(inputRow, this::collect);
                        break;
                    }
                    case 1: {
                        doubleInputOperation.applySecond(inputRow, this::collect);
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unknown gate");
                    }
                }
            }
        }
    }

    public final void pushIntoZero(Row inputRow) {
        push(inputRow, 0);
    }

    private void collect(Row row) {
        for (NodeGateInfo info : nextNodes) {
            SparkNode node = info.getNode();
            int gateNumber = info.getGateNumber();
            node.push(row, gateNumber);
        }
    }
}
