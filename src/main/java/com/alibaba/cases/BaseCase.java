package com.alibaba.cases;

import com.alibaba.GraphBuilder;
import com.alibaba.Row;
import com.alibaba.nodes.CompNode;
import com.alibaba.ops.mappers.LambdaMapper;
import com.alibaba.ops.mappers.Printer;
import com.alibaba.ops.mappers.WordSplitMapper;
import com.alibaba.ops.reducers.CountReducer;
import com.alibaba.ops.reducers.FirstNReducer;

import java.util.List;

import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.pushAllThenTerminal;
import static com.alibaba.ops.reducers.Sorter.Order.ASCENDING;
import static com.alibaba.ops.reducers.Sorter.Order.DESCENDING;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BaseCase implements TestCase {

    @Override
    public void launch() {
        CompNode headGraphNode = createGraph().get(0);

        List<Row> inputRows = convertToRows("Doc", INPUT_VALUES);

        pushAllThenTerminal(headGraphNode, inputRows);
    }

    @Override
    public List<CompNode> createGraph() {
        GraphBuilder graphBuilder = GraphBuilder
                .startWith(new WordSplitMapper("Doc", "Word"))
                .then(new LambdaMapper<String>("Word", String::toLowerCase))
                .sortBy(ASCENDING, "Word")
                .then(new CountReducer("WordCount", "Word"));

        graphBuilder
                .branch()
                .sortBy(DESCENDING, "WordCount")
                .then(new FirstNReducer(5))
                .then(new Printer("+++ Top 5 common words"));

        graphBuilder
                .branch()
                .sortBy(ASCENDING, "WordCount")
                .then(new FirstNReducer(10))
                .then(new Printer("--- Top 10 rare words"));

        return singletonList(
                graphBuilder.getStartNode()
        );
    }

    private static final List<String> INPUT_VALUES = asList(
            "I am the Emperor's will made manifest",
            "Faith is purest when it is unquestioned",
            "I accept the challenge",
            "By the Emperor, it shall be so!",
            "Bless the mind too small for doubt",
            "Your wise counsel belies your years",
            "Difficult, to be sure... but it shall be so",
            "I shall rally the troops to your cause!",
            "May you lead us to victory!",
            "Redeem them with sword and fire!",
            "Brothers, we are together again!",
            "Rally to me, brothers, and we will win!",
            "Drive them back!",
            "To the last, kill them all!",
            "Only in war are we truly faithful!",
            "May the Emperor watch over us all!",
            "You cannot stand against my Faith!",
            "For the glory of the Imperium, CHARGE!",
            "Break the enemy line!",
            "We live and die as brothers!",
            "Pain now! Reward in the afterlife!",
            "Our duty lies elsewhere so be quick!",
            "Spirit us to our objective, driver",
            "So this is where the Emperor's will brings us",
            "Ahh, to walk upon the bloodstained ground",
            "We have been humbled",
            "Retreat, heed me and regroup!",
            "I hear the drums of war again!",
            "Make a stand! Here and now!",
            "None can withstand our faith!",
            "Mighty Emperor, guide my blow!",
            "Come all you xeno scum and fallen heretics, come and face the one true might of the universe and wither under the Golden Throne's gaze!",
            "Take them, and quickly!"
    );
}
