package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtwithtrees", DistrictwithTrees.class,
                    "A map/reduce program that returns the distinct districts with trees");
            programDriver.addClass("heightsortedtrees", HeightSortedTrees.class,
                    "A map/reduce program that sort trees by height");
            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that returns the oldest tree of all districts");
            programDriver.addClass("treesbykind", TreesbyKind.class,
                    "A map/reduce program that returns the trees sorted by kind");
            programDriver.addClass("treesmaxheight", TreesMaxHeight.class,
                    "A map/reduce program that returns the maximum height per kind of tree");
            programDriver.addClass("treespecies", TreeSpecies.class,
                    "A map/reduce program that returns the different species of trees");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
