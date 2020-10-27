package com.alibaba;


import com.alibaba.nodes.DoubleInputNode;
import com.alibaba.nodes.SingleInputNode;
import com.alibaba.ops.DumbJoin;
import com.alibaba.ops.single.Count;
import com.alibaba.ops.single.FirstNReduce;
import com.alibaba.ops.single.LambdaMap;
import com.alibaba.ops.single.Print;
import com.alibaba.ops.single.Sort;
import com.alibaba.ops.single.WordSplitMap;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.Utils.appendOperations;
import static com.alibaba.Utils.chainOperations;
import static com.alibaba.Utils.convertToRows;
import static com.alibaba.Utils.getLastNode;
import static com.alibaba.ops.single.Sort.Order.ASCENDING;
import static com.alibaba.ops.single.Sort.Order.DESCENDING;
import static java.util.Arrays.asList;

public class Main {

    public static void main(String[] args) {
        SingleInputNode headGraphNode = chainOperations(
                new WordSplitMap(),
                new LambdaMap<String, String>(String::toLowerCase, 0),
                new LambdaMap<String, String>(w -> w.replaceAll("[\\.\\'\\,\\!]", ""), 0),
                new Sort(ASCENDING, 0),
                new Count(0)
        );
        SingleInputNode splitGraphNode = getLastNode(headGraphNode);

        appendOperations(
                splitGraphNode,
                new Sort(DESCENDING, 1),
                new FirstNReduce(5),
                new Print("+++ Top 5 common words")
        );

        appendOperations(
                splitGraphNode,
                new Sort(ASCENDING, 1),
                new FirstNReduce(10),
                new Print("--- Top 10 rare words")
        );

        List<Row> inputRows = convertToRows(INPUT_VALUES);
        for (Row row : inputRows) {
            headGraphNode.process(row);
        }
        headGraphNode.process(Row.terminalRow());

//        SingleInputNode leftInput = chainOperations(
//                new Print("+++ Left: ")
//        );
//
//        SingleInputNode rightInput = chainOperations(
//                new Print("*** Right: ")
//        );
//
//        DoubleInputNode joinNode = new DoubleInputNode(new DumbJoin());
//        getLastNode(leftInput).getNextNodes().add(joinNode.getLeft());
//        getLastNode(rightInput).getNextNodes().add(joinNode.getRight());
//
//        joinNode.setNextStep(
//                new SingleInputNode(
//                        new Print("--- Output: ")
//                )
//        );
//
//        List<Row> leftInputRows = convertToRows(INPUT_VALUES);
//        List<Row> rightInputRows = convertToRows(INPUT_NUMBERS);
//
//        for (int index = 0; index < 10; index++) {
//            leftInput.process(leftInputRows.get(index));
//            rightInput.process(rightInputRows.get(index));
//        }
//        leftInput.process(Row.terminalRow());
//        rightInput.process(Row.terminalRow());
    }

    public static final List<String> INPUT_VALUES = asList(
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

    public static final List<Integer> INPUT_NUMBERS = IntStream
            .range(0, 20)
            .boxed()
            .collect(Collectors.toList());
}
