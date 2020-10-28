package com.alibaba.ops;

import com.alibaba.Row;

public class OpUtils {
    public static boolean equalByColumns(Row left, Row right, String... comparisonColumns) {
        for (String column : comparisonColumns) {
            Object leftValue = left.get(column);
            Object rightValue = right.get(column);
            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }
}
