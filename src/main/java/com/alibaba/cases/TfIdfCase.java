package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.nodes.CompNode;
import com.alibaba.ops.InnerJoin;
import com.alibaba.ops.mappers.AddColumnMapper;
import com.alibaba.ops.mappers.IdentityMapper;
import com.alibaba.ops.mappers.LambdaMapper;
import com.alibaba.ops.mappers.Printer;
import com.alibaba.ops.mappers.RetainColumnsMapper;
import com.alibaba.ops.mappers.WordSplitMapper;
import com.alibaba.ops.reducers.CountReducer;
import com.alibaba.ops.reducers.FirstNReducer;
import com.alibaba.ops.reducers.TermFrequencyReducer;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;
import static com.alibaba.ops.reducers.Sorter.Order.ASCENDING;
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
                .startWith(new IdentityMapper());

        GraphBuilder docCountGraph = inputGraph
                .branch()
                .sortBy(ASCENDING)
                .then(new CountReducer("DocsCount"));

        GraphBuilder wordGraph = inputGraph
                .branch()
                .then(new LambdaMapper<String>("Text", String::toLowerCase))
                .then(new WordSplitMapper("Text", "Word"));

        GraphBuilder uniqueDocWordGraph = wordGraph
                .branch()
                .sortBy(ASCENDING, "Id", "Word")
                .then(new FirstNReducer(1, "Id", "Word"))
                .sortBy(ASCENDING, "Word");

        GraphBuilder countIdfGraph = uniqueDocWordGraph
                .branch()
                .then(new CountReducer("DocsWithWordCount", "Word"))
                .join(uniqueDocWordGraph, new InnerJoin("Word"))
                .sortBy(ASCENDING, "Id", "Word");

        GraphBuilder finalGraph = wordGraph
                .branch()
                .sortBy(ASCENDING, "Id")
                .then(new TermFrequencyReducer("Word", "Tf", "Id"))
                .join(countIdfGraph, new InnerJoin("Id", "Word"))
                .join(docCountGraph, new InnerJoin())
                .then(new AddColumnMapper("TfIdf", TfIdfCase::calculateTfIdf))
                .then(new RetainColumnsMapper("Id", "Word", "TfIdf"))
                .then(new Printer("^^^ final: "));

        return asList(
                inputGraph.getStartNode()
        );
    }

    public static double calculateTfIdf(Row row) {
        double tf = row.getDouble("Tf");
        double docsCount = row.getDouble("DocsCount");
        double docsWithWordCount = row.getDouble("DocsWithWordCount");

        return tf * Math.log(docsCount / docsWithWordCount);
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
