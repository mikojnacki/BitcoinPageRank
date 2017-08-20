package com.mikolaj.app;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.Ranking;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Credits to Jimmy Lin @lintool
 */
public class SequentialPageRank {

    private SequentialPageRank() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-jump", metaVar = "[num]", usage = "random jump factor")
        float alpha = 0.15f;
    }

    public static void main(String[] argv) throws IOException {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(-1);
        }

        int edgeCnt = 0;
        DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<>();

        BufferedReader data =
                new BufferedReader(new InputStreamReader(new FileInputStream(args.input)));

        String line;
        while ((line = data.readLine()) != null) {
            //String[] arr = line.split("\\t");
            String[] arr = line.split("\\s+");

            for (int i = 1; i < arr.length; i++) {
                graph.addEdge(edgeCnt++, arr[0], arr[i]);
            }
        }

        data.close();

        WeakComponentClusterer<String, Integer> clusterer = new WeakComponentClusterer<>();
        Set<Set<String>> components = clusterer.apply(graph);

        System.out.println("Number of components: " + components.size());
        System.out.println("Number of edges: " + graph.getEdgeCount());
        System.out.println("Number of nodes: " + graph.getVertexCount());
        System.out.println("Random jump factor: " + args.alpha);

        // Compute PageRank.
        PageRank<String, Integer> ranker = new PageRank<>(graph, args.alpha);
        ranker.evaluate();

        // Use priority queue to sort vertices by PageRank values.
        PriorityQueue<Ranking<String>> q = new PriorityQueue<>();
        int i = 0;
        for (String pmid : graph.getVertices()) {
            q.add(new Ranking<>(i++, ranker.getVertexScore(pmid), pmid));
        }

        // Print PageRank values.
        System.out.println("\nPageRank of nodes, in descending order:");
        Ranking<String> r;
//        while ((r = q.poll()) != null) {
//            System.out.println(String.format("%s\t%.5f ", r.getRanked(), Math.log(r.rankScore)));
//        }


        // Pring only frist 1000
        int count = 0;
        while ((r = q.poll()) != null) {
            if (count == 1000) {
                break;
            }
            System.out.println(String.format("%s\t%.5f ", r.getRanked(), Math.log(r.rankScore)));
            count = count + 1;
        }

    }
}
