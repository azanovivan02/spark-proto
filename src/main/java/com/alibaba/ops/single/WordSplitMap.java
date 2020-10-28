package com.alibaba.ops.single;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;
import com.alibaba.ops.Operation;

public class WordSplitMap implements Operation {

    private String inputColumn;
    private String outputColumn;

    public WordSplitMap(String inputColumn, String outputColumn) {
        this.inputColumn = inputColumn;
        this.outputColumn = outputColumn;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        String inputValue = inputRow.getString(inputColumn);
        if (inputValue == null) {
            System.out.println("hEllo");
        }
        String[] words = inputValue.split(" ");
        for (String word : words) {
            Row newRow = inputRow
                    .copyColumnsExcept(inputColumn)
                    .set(outputColumn, word);
            collector.collect(newRow);
        }
    }
}
