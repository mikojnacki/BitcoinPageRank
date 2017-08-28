package com.mikolaj.app;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Modified by Mikolaj Chojnacki
 * Credits to Jimmy Lin @lintool
 */
public class RunPageRankBasic extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPageRankBasic.class);

    private static enum PageRank {
        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    };

    // Mapper, no in-mapper combining.
    private static class MapClass extends Mapper<Text, PageRankNode, Text, PageRankNode> {

        // The neighbor to which we're sending messages.
        private static final Text neighbor = new Text();

        // Contents of the messages: partial PageRank mass.
        private static final PageRankNode intermediateMass = new PageRankNode();

        // For passing along node structure.
        private static final PageRankNode intermediateStructure = new PageRankNode();

        @Override
        public void map(Text nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            // DEBUG - Check consumed key - value records
            //LOG.info("key (nid): " + nid.toString());
            //LOG.info("value (type): " + node.getType().toString());
            //LOG.info("value (nodeid): " + node.getNodeId().toString());
            //LOG.info("value (pagerank): " + String.valueOf(node.getPageRank()));
            //LOG.info("value (adjacencyList): " + node.getAdjacenyList().toString());
            //LOG.info("<PageRankNode> value.toString(): " + node.toString());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets a proportional share of PageRank mass - based on weight
                ArrayListWritable<Text> list = node.getAdjacenyList();
                float mass;
                long amount;
                long outSum;

                context.getCounter(PageRank.edges).increment(list.size());

                // Iterate over neighbors
                for (int i = 0; i < list.size(); i++) {
                    // get amount and outSum from neighbor
                    amount = Long.valueOf(list.get(i).toString().trim().split("\\|")[1]); // consider try catch
                    outSum = Long.valueOf(list.get(i).toString().trim().split("\\|")[2]); // consider try catch
                    // calculate proportional mass
                    mass = node.getPageRank() + (float) StrictMath.log(amount) - (float) StrictMath.log(outSum);

                    neighbor.set(list.get(i).toString().trim().split("\\|")[0]);
                    intermediateMass.setNodeId(new Text(list.get(i).toString().trim().split("\\|")[0]));
                    intermediateMass.setType(PageRankNode.Type.Mass);
                    intermediateMass.setPageRank(mass);

                    // Emit messages with PageRank mass to neighbors.
                    context.write(neighbor, intermediateMass);
                    massMessages++;
                }
            }

            // Bookkeeping.
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class ReduceClass extends
            Reducer<Text, PageRankNode, Text, PageRankNode> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private float totalMass = Float.NEGATIVE_INFINITY;

        @Override
        public void reduce(Text nid, Iterable<PageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PageRankNode node = new PageRankNode();

            node.setType(PageRankNode.Type.Complete);
            node.setNodeId(nid);

            int massMessagesReceived = 0;
            int structureReceived = 0;

            float mass = Float.NEGATIVE_INFINITY;
            while (values.hasNext()) {
                PageRankNode n = values.next();

                if (n.getType().equals(PageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListWritable<Text> list = n.getAdjacenyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    mass = sumLogProbs(mass, n.getPageRank());
                    massMessagesReceived++;
                }
            }

            // Update the final accumulated PageRank mass.
            node.setPageRank(mass);
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
                totalMass = sumLogProbs(totalMass, mass);
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.toString() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.toString()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            // Write to a file the amount of PageRank mass we've seen in this reducer.
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
            out.writeFloat(totalMass);
            out.close();
        }
    }

    // ########################################################################################################

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    private static class MapPageRankMassDistributionClass extends
            Mapper<Text, PageRankNode, Text, PageRankNode> {
        private float missingMass = 0.0f;
        private int nodeCnt = 0;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            missingMass = conf.getFloat("MissingMass", 0.0f);
            nodeCnt = conf.getInt("NodeCount", 0);
        }

        @Override
        public void map(Text nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            float p = node.getPageRank();

            float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
            float link = (float) Math.log(1.0f - ALPHA)
                    + sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));

            p = sumLogProbs(jump, link);
            node.setPageRank(p);

            context.write(nid, node);
        }
    }

    // Random jump factor.
    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPageRankBasic(), args);
    }


    public RunPageRankBasic() {}

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
    private static final String COMBINER = "useCombiner";
    private static final String INMAPPER_COMBINER = "useInMapperCombiner";
    private static final String RANGE = "range";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(COMBINER, "use combiner"));
        options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
        options.addOption(new Option(RANGE, "use range partitioner"));

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        boolean useCombiner = cmdline.hasOption(COMBINER);
        boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
        boolean useRange = cmdline.hasOption(RANGE);

        LOG.info("Tool name: RunPageRank");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info(" - use combiner: " + useCombiner);
        LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
        LOG.info(" - user range partitioner: " + useRange);

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner);
        }

        return 0;
    }

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes,
                                 boolean useCombiner, boolean useInMapperCombiner) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        float mass = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner);

        // Find out how much PageRank mass got lost at the dangling nodes.
        float missing = 1.0f - (float) StrictMath.exp(mass);

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(i, j, missing, basePath, numNodes);
    }

    private float phase1(int i, int j, String basePath, int numNodes,
                         boolean useCombiner, boolean useInMapperCombiner) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - nodeCnt: " + numNodes);
        LOG.info(" - useCombiner: " + useCombiner);
        LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        job.getConfiguration().set("PageRankMassPath", outm);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapClass.class);

//        job.setMapperClass(useInMapperCombiner ? MapWithInMapperCombiningClass.class : MapClass.class);
//
//        if (useCombiner) {
//            job.setCombinerClass(CombineClass.class);
//        }

        job.setReducerClass(ReduceClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);
        FileSystem.get(getConf()).delete(new Path(outm), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        float mass = Float.NEGATIVE_INFINITY;
        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus f : fs.listStatus(new Path(outm))) {
            FSDataInputStream fin = fs.open(f.getPath());
            mass = sumLogProbs(mass, fin.readFloat());
            fin.close();
        }

        return mass;
    }

    private void phase2(int i, int j, float missing, String basePath, int numNodes) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPageRankBasic.class);

        LOG.info("missing PageRank mass: " + missing);
        LOG.info("number of nodes: " + numNodes);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setFloat("MissingMass", (float) missing);
        job.getConfiguration().setInt("NodeCount", numNodes);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }

}
