package com.alibaba.nodes;

import com.alibaba.Row;
import com.alibaba.ops.DoubleInputOperation;

public class DoubleInputNode {

    private final DoubleInputOperation operation;
    private InputProcessor nextStep;

    public DoubleInputNode(DoubleInputOperation operation) {
        this.operation = operation;
    }

    public DoubleInputOperation getOperation() {
        return operation;
    }

    public InputProcessor getNextStep() {
        return nextStep;
    }

    public void setNextStep(InputProcessor nextStep) {
        this.nextStep = nextStep;
    }

    public InputProcessor getLeft() {
        return this::processLeft;
    }

    public InputProcessor getRight() {
        return this::processRight;
    }

    private void processLeft(Row inputRow) {
        operation.applyLeft(inputRow, DoubleInputNode.this::collect);
    }

    private void processRight(Row inputRow) {
        operation.applyRight(inputRow, DoubleInputNode.this::collect);
    }

    private void collect(Row row) {
        if (nextStep != null) {
            nextStep.process(row);
        }
    }
}
