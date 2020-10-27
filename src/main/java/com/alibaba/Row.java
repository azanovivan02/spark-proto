package com.alibaba;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class Row {

    private List<Object> values = new ArrayList<>();

    public Row(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public Object get(int index) {
        return values.get(index);
    }

    public String getString(int index) {
        return (String) values.get(index);
    }

    public Row copy() {
        return new Row(new ArrayList<>(values));
    }

    public Row replaceAndCopy(int index, Object value) {
        ArrayList<Object> newValues = new ArrayList<>(this.values);
        newValues.set(index, value);
        return new Row(newValues);
    }

    public Row addAndCopy(Object value) {
        ArrayList<Object> newValues = new ArrayList<>(this.values);
        newValues.add(value);
        return new Row(newValues);
    }

    public boolean isTerminal(){
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
