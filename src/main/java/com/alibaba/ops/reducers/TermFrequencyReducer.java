package com.alibaba.ops.reducers;

import com.alibaba.Row;
import com.alibaba.nodes.OutputCollector;
import com.alibaba.ops.OpUtils;
import com.alibaba.ops.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TermFrequencyReducer implements Operator.Reducer {

    private String termColumn;
    private String outputColumn;
    private final String[] groupByColumns;

    private Row currentRow = null;
    private Map<String, Integer> wordCounts = new HashMap<>();

    public TermFrequencyReducer(String termColumn, String outputColumn, String... groupByColumns) {
        this.termColumn = termColumn;
        this.outputColumn = outputColumn;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void apply(Row inputRow, OutputCollector collector) {
        if (inputRow.isTerminal()) {
            outputTfRows(collector);
            currentRow = null;
            wordCounts.clear();
            collector.collect(inputRow);
            return;
        }

        if (!OpUtils.equalByColumns(inputRow, currentRow, groupByColumns)) {
            outputTfRows(collector);
            wordCounts.clear();
            currentRow = inputRow;
        }

        String currentWord = inputRow.getString(termColumn);
        Integer currentCount = wordCounts.getOrDefault(currentWord, 0);
        wordCounts.put(currentWord, currentCount + 1);
    }

    private void outputTfRows(OutputCollector collector) {
        if (currentRow == null) {
            return;
        }

        int totalCount = getTotalCount();
        TreeMap<String, Integer> sortedWordCounts = new TreeMap<>(wordCounts);

        for (Entry<String, Integer> entry : sortedWordCounts.entrySet()) {
            String term = entry.getKey();
            Integer termCount = entry.getValue();
            float frequency = ((float) termCount) / totalCount;
            Row newRow = currentRow
                    .copyColumns(groupByColumns)
                    .set(termColumn, term)
                    .set(outputColumn, frequency);
            collector.collect(newRow);
        }

    }

    private int getTotalCount() {
        int totalCount = 0;
        for (Integer count : wordCounts.values()) {
            totalCount += count;
        }
        return totalCount;
    }
}
