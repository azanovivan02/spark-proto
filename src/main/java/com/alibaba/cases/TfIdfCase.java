package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.nodes.CompNode;
import com.alibaba.ops.InnerJoin;
import com.alibaba.ops.single.Count;
import com.alibaba.ops.single.FirstNReduce;
import com.alibaba.ops.single.LambdaMap;
import com.alibaba.ops.single.Print;
import com.alibaba.ops.single.Sort;
import com.alibaba.ops.single.WordSplitMap;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;
import static com.alibaba.ops.single.Sort.Order.ASCENDING;
import static java.util.Arrays.asList;

public class TfIdfCase implements TestCase {

    @Override
    public void launch() {
        CompNode startNode = createGraph().get(0);

        pushAllThenTerminal(startNode, inputRows);
    }

    @Override
    public List<CompNode> createGraph() {
        GraphBuilder inputGraph = GraphBuilder
                .startWith(new Print("+++ input: "));

        inputGraph
                .branch()
                .then(new Sort(ASCENDING))
                .then(new Count("DocsCount"));

        GraphBuilder wordGraph = inputGraph
                .branch()
                .then(new LambdaMap<String, String>("Text", String::toLowerCase))
                .then(new WordSplitMap("Text", "Word"));

        GraphBuilder uniqueDocWordGraph = wordGraph
                .branch()
                .then(new Sort(ASCENDING, "Id", "Word"))
                .then(new FirstNReduce(1, "Id", "Word"))
                .then(new Sort(ASCENDING, "Word"))
                .then(new Print("^^^ unique doc word: "));

        GraphBuilder countIdfGraph = uniqueDocWordGraph
                .branch()
                .then(new Count("DocsWithWordCount", "Word"))
                .then(new Print("--- DocsWithWordCount: "))
                .join(uniqueDocWordGraph, new InnerJoin("Word"))
                .then(new Sort(ASCENDING, "Id", "Word"))
                .then(new Print("^^^ idf: "));

        return asList(
                inputGraph.getStartNode()
        );
    }

    private static final List<Row> inputRows = convertToRows(
            new String[]{"Id", "Text"},
            new Object[][]{
                    {1, "hello, little world"},
                    {2, "little"},
                    {3, "little little little"},
                    {4, "little? hello little world"},
                    {5, "HELLO HELLO! WORLD..."},
                    {6, "world? world... world!!! WORLD!!! HELLO!!!"}
            }
    );
}
