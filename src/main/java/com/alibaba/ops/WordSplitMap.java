package com.alibaba.ops;

import com.alibaba.nodes.OutputCollector;
import com.alibaba.Row;

public class WordSplitMap implements SingleInputOperation {

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        String inputValue = inputRow.getString(0);
        String[] words = inputValue.split(" ");
        for (String word : words) {
            Row newRow = inputRow.replaceAndCopy(0, word);
            collector.collect(newRow);
        }
    }
}
