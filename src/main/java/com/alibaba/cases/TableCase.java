package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.single.Count;
import com.alibaba.ops.single.Print;
import com.alibaba.ops.single.Sort;
import com.alibaba.ops.single.WordSplitMap;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;
import static com.alibaba.ops.single.Sort.Order.ASCENDING;

public class TableCase {

    public static void processCaseDocument() {
        SparkNode startNode = GraphBuilder
                .startWith(new WordSplitMap("Text", "Word"))
                .then(new Sort(ASCENDING, "Author", "Word"))
                .then(new Count("Author", "Word"))
                .then(new Print("+++ "))
                .getStartNode();

        pushAllThenTerminal(startNode, inputRows);
    }

    private static final List<Row> inputRows = convertToRows(
            new String[]{"Id", "Text", "Author"},
            new Object[][]{
                    {1, "The Grey Knights have come on behalf of the Holy Inquisition.", "Grey Knights"},
                    {2, "The enemies of the Emperor shall be destroyed!", "Apothecary"},
                    {3, "The warriors of the Inquisition are yours to command", "Grey Knights"},
                    {4, "The Heretics will suffer the ultimate punishment!", "Grey Knights"},
                    {5, "The fallen shall be forever remembered as the Emperor's finest.", "Apothecary"}
            }
    );
}
