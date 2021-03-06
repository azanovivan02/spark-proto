package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.nodes.CompNode;
import com.alibaba.ops.InnerJoin;
import com.alibaba.ops.mappers.Printer;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;
import static java.util.Arrays.asList;

public class JoinCase implements TestCase {

    @Override
    public void launch() {
        List<CompNode> startNodes = createGraph();

        CompNode leftStartNode = startNodes.get(0);
        CompNode rightStartNode = startNodes.get(1);

        pushAllThenTerminal(leftStartNode, leftRows);
        pushAllThenTerminal(rightStartNode, rightRows);
    }

    @Override
    public List<CompNode> createGraph() {
        GraphBuilder rightGraphBuilder = GraphBuilder
                .startWith(new Printer("--- right: "));

        GraphBuilder leftGraphBuilder = GraphBuilder
                .startWith(new Printer("+++ left: "))
                .join(rightGraphBuilder, new InnerJoin("AuthorId"))
                .then(new Printer("*** output: "));

        return asList(
                leftGraphBuilder.getStartNode(),
                rightGraphBuilder.getStartNode()
        );
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
