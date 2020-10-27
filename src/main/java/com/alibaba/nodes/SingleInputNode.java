package com.alibaba.nodes;

import com.alibaba.Row;
import com.alibaba.ops.SingleInputOperation;
import com.alibaba.ops.TerminalAwareOperation;

import java.util.ArrayList;
import java.util.List;

public class SingleInputNode implements InputProcessor {

    private final SingleInputOperation operation;
    private final boolean isTerminalAware;
    private final List<InputProcessor> nextNodes;

    public SingleInputNode(SingleInputOperation operation) {
        this.operation = operation;
        this.isTerminalAware = (operation instanceof TerminalAwareOperation);
        this.nextNodes = new ArrayList<>();
    }

    public SingleInputOperation getOperation() {
        return operation;
    }

    public List<InputProcessor> getNextNodes() {
        return nextNodes;
    }

    @Override
    public void process(Row inputRow) {
        if (isTerminalAware || !inputRow.isTerminal()) {
            operation.apply(inputRow, this::collect);
        } else {
            collect(inputRow);
        }
    }

    private void collect(Row row) {
        for (InputProcessor node : nextNodes) {
            node.process(row);
        }
    }
}
