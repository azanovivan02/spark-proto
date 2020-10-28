package com.alibaba.ops;

import com.alibaba.Row;

public class OpUtils {
    public static boolean equalByColumns(Row left, Row right, String... comparisonColumns) {
        if (left == null || right == null) {
            return false;
        }

        for (String column : comparisonColumns) {
            Object leftValue = left.get(column);
            Object rightValue = right.get(column);
            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }

    public static int compareRows(Row o1, Row o2, String[] keyColumns) {
        for (String column : keyColumns) {
            Comparable leftValue = o1.getComparable(column);
            Comparable rightValue = o2.getComparable(column);
            int comparisonResult = leftValue.compareTo(rightValue);
            if (comparisonResult != 0) {
                return comparisonResult;
            }
        }

        return 0;
    }
}
