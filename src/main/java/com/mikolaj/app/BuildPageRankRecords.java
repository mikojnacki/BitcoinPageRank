package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Mikolaj on 16.08.17.
 */
public class BuildPageRankRecords extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildPageRankRecords.class);

    private static final String NODE_CNT_FIELD = "node.cnt";

    private static class MyMapper extends Mapper<LongWritable, Text, Text, PageRankNode> {
        private static final Text nid = new Text();
        private static final PageRankNode node = new PageRankNode();

        @Override
        public void setup(Mapper<LongWritable, Text, Text, PageRankNode>.Context context) {
            int n = context.getConfiguration().getInt(NODE_CNT_FIELD, 0);
            if (n == 0) {
                throw new RuntimeException(NODE_CNT_FIELD + " cannot be 0!");
            }
            node.setType(PageRankNode.Type.Complete);
            node.setPageRank((float) -StrictMath.log(n));
        }

        @Override
        public void map(LongWritable key, Text t, Context context) throws IOException,
                InterruptedException {
            // added LOG.info to debug
            String[] arr = t.toString().trim().split("\\s+");

            nid.set(arr[0]);
            LOG.info("nid: " + nid.toString());
            if (arr.length == 1) {
                node.setNodeId(new Text(String.valueOf(arr[0])));
                LOG.info("node id: " + node.getNodeId().toString());
                node.setAdjacencyList(new ArrayListWritable<Text>());
                LOG.info("node list: " + node.getAdjacenyList().toString());

            } else {
                node.setNodeId(new Text(String.valueOf(arr[0])));
                LOG.info("node id: " + node.getNodeId().toString());

//                int[] neighbors = new int[arr.length - 1];
//                for (int i = 1; i < arr.length; i++) {
//                    neighbors[i - 1] = Integer.parseInt(arr[i]);
//                }

                ArrayList<Text> neighbors = new ArrayList<>();
                for (int i = 1; i < arr.length; i++) {
                    LOG.info("neighbor candidate: " + arr[i]);
                    neighbors.add(new Text(arr[i]));
                }

                node.setAdjacencyList(new ArrayListWritable<>(neighbors));
                LOG.info("node list: " + node.getAdjacenyList().toString());
            }

            context.getCounter("graph", "numNodes").increment(1);
            context.getCounter("graph", "numEdges").increment(arr.length - 1);

            if (arr.length > 1) {
                context.getCounter("graph", "numActiveNodes").increment(1);
            }
            LOG.info("finally nid: " + nid.toString());
            context.write(nid, node);
        }
    }

    public BuildPageRankRecords() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_NODES = "numNodes";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of nodes").create(NUM_NODES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));

        LOG.info("Tool name: " + BuildPageRankRecords.class.getSimpleName());
        LOG.info(" - inputDir: " + inputPath);
        LOG.info(" - outputDir: " + outputPath);
        LOG.info(" - numNodes: " + n);

        Configuration conf = getConf();
        conf.setInt(NODE_CNT_FIELD, n);
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(BuildPageRankRecords.class.getSimpleName() + ":" + inputPath);
        job.setJarByClass(BuildPageRankRecords.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MyMapper.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildPageRankRecords(), args);
    }
}
