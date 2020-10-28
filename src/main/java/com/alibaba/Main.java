package com.alibaba;


import com.alibaba.cases.BaseCase;
import com.alibaba.cases.JoinCase;
import com.alibaba.cases.TableCase;
import com.alibaba.cases.TestCase;
import com.alibaba.nodes.Node;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.ui.GraphVisualizer.visualizeGraph;

public class Main {

    public static void main(String[] args) {
        List<TestCase> testCases = Arrays.asList(
                new BaseCase(),
                new TableCase(),
                new JoinCase()
        );
        for (TestCase testCase : testCases) {
            testCase.launch();
            System.out.println("\n=============\n");
        }

        Node baseCaseGraph = testCases.get(1).createGraph();
        visualizeGraph(baseCaseGraph);
    }

}
