package com.alibaba.nodes;

import com.alibaba.Row;
import com.alibaba.ops.DoubleInputOperation;
import com.alibaba.ops.OpType;
import com.alibaba.ops.Operation;
import com.alibaba.ops.TerminalAwareOperation;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.ops.OpType.DOUBLE_INPUT;
import static com.alibaba.ops.OpType.SINGLE_INPUT;
import static com.alibaba.ops.OpType.TERMINAL_AWARE;

public class CompNode {

    private final Operation operation;
    private final OpType opType;
    private final List<Connection> nextNodes;

    public CompNode(Operation operation) {
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

    public List<Connection> getNextNodes() {
        return nextNodes;
    }

    public void addConnection(CompNode node, int gate) {
        Connection connection = new Connection(node, gate);
        nextNodes.add(connection);
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
        for (Connection info : nextNodes) {
            CompNode node = info.getNode();
            int gateNumber = info.getGate();
            node.push(row, gateNumber);
        }
    }
}
