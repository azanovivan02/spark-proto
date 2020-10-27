package com.alibaba;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class Row {

    private Map<String, Object> values;

    public Row(Map<String, Object> values) {
        this.values = values;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Object getSingle() {
        return values.values().iterator().next();
    }

    public String getSingleString() {
        return (String) getSingle();
    }

    public Object get(String columnName) {
        return values.get(columnName);
    }

    public String getString(String columnName) {
        return (String) get(columnName);
    }

    public Row set(String columnName, Object value) {
        this.values.put(columnName, value);
        return this;
    }

    public Row copy() {
        return new Row(
                new LinkedHashMap<>(values)
        );
    }

    public Row copyColumns(String...columns) {
        LinkedHashMap<String, Object> newValues = new LinkedHashMap<>();
        for (String column : columns) {
            newValues.put(
                    column,
                    values.get(column)
            );
        }
        return new Row(newValues);
    }

    public Row copyColumnsExcept(String...excludedColumns) {
        LinkedHashMap<String, Object> newValues = new LinkedHashMap<>(values);
        newValues
                .keySet()
                .removeAll(Arrays.asList(excludedColumns));
        return new Row(newValues);
    }

    public boolean isTerminal() {
        return values == null;
    }

    public static Row terminalRow() {
        return new Row(null);
    }

    @Override
    public String toString() {
        return format("Row %s", values);
    }
}
