package com.alibaba;


import com.alibaba.nodes.SparkNode;

import static com.alibaba.cases.BaseCase.createBaseCaseGraph;
import static com.alibaba.cases.BaseCase.processBaseCase;
import static com.alibaba.cases.JoinCase.processJoinCase;
import static com.alibaba.cases.TableCase.processCaseDocument;
import static com.alibaba.ui.GraphVisualizer.visualizeGraph;

public class Main {

    public static void main(String[] args) {
        System.out.println("=============");
        processBaseCase();
        System.out.println("=============");
        processCaseDocument();
        System.out.println("=============");
        processJoinCase();

        SparkNode baseCaseGraph = createBaseCaseGraph();
        visualizeGraph(baseCaseGraph);
    }

}
