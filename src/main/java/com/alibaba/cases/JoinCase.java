package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.Utils;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.InnerJoin;
import com.alibaba.ops.single.Print;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;

public class JoinCase {

    public static void processJoinCase() {
        GraphBuilder rightGraphBuilder = GraphBuilder
                .startWith(new Print("--- right: "));

        GraphBuilder leftGraphBuilder = GraphBuilder
                .startWith(new Print("+++ left: "))
                .join(rightGraphBuilder, new InnerJoin("AuthorId"))
                .then(new Print("*** output: "));

        SparkNode leftStartNode = leftGraphBuilder
                .getStartNode();
        SparkNode rightStartNode = rightGraphBuilder
                .getStartNode();

        pushAllThenTerminal(leftStartNode, leftRows);
        pushAllThenTerminal(rightStartNode, rightRows);
    }


    private static final List<Row> leftRows = convertToRows(
            new String[]{"DocId", "Text", "AuthorId"},
            new Object[][]{
                    {1, "The Grey Knights have come on behalf of the Holy Inquisition.", 100},
                    {4, "The Heretics will suffer the ultimate punishment!", 100},
                    {2, "The enemies of the Emperor shall be destroyed!", 200},
                    {3, "The warriors of the Inquisition are yours to command", 100},
                    {5, "The fallen shall be forever remembered as the Emperor's finest.", 200}
            }
    );

    private static final List<Row> rightRows = convertToRows(
            new String[]{"AuthorId", "AuthorName"},
            new Object[][]{
                    {100, "Grey Knights"},
                    {100, "Tactical"},
                    {200, "Apothecary"}
            }
    );
}
