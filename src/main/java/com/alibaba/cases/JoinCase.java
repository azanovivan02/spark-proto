package com.alibaba.cases;

import com.alibaba.Row;
import com.alibaba.nodes.NodeGateInfo;
import com.alibaba.nodes.SparkNode;
import com.alibaba.ops.InnerJoin;
import com.alibaba.ops.single.Print;

import java.util.List;

import static com.alibaba.Utils.chainOperations;
import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.getLastNode;

public class JoinCase {

    public static void processJoinCase() {
        SparkNode leftGraph = chainOperations(
                new Print("+++ left: ")
        );
        SparkNode rightGraph = chainOperations(
                new Print("--- right: ")
        );

        SparkNode joinNode = new SparkNode(new InnerJoin("AuthorId"));
        getLastNode(leftGraph).getNextNodes().add(new NodeGateInfo(joinNode, 0));
        getLastNode(rightGraph).getNextNodes().add(new NodeGateInfo(joinNode, 1));

        joinNode.getNextNodes().add(
                new NodeGateInfo(
                        new SparkNode(new Print("*** output: ")),
                        0
                )
        );

        for (Row leftRow : leftRows) {
            leftGraph.push(leftRow, 0);
        }
        leftGraph.push(Row.terminalRow(), 0);

        for (Row rightRow : rightRows) {
            rightGraph.push(rightRow, 0);
        }
        rightGraph.push(Row.terminalRow(), 0);
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
