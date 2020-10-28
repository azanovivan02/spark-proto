package com.alibaba;

import com.alibaba.nodes.SparkNode;

import java.util.ArrayList;
import java.util.Collection;
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

    public static void pushAllThenTerminal(SparkNode node, List<Row> rows) {
        for (Row row : rows) {
            node.pushIntoZero(row);
        }
        node.pushIntoZero(Row.terminalRow());
    }
}
