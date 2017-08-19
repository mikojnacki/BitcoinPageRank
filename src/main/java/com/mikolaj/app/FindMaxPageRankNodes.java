package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Mikolaj on 19.08.17.
 */
public class FindMaxPageRankNodes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(FindMaxPageRankNodes.class);

    private static class MyMapper extends
            Mapper<Text, PageRankNode, Text, FloatWritable> {
        private TopScoredObjects<String> queue;

        @Override
        public void setup(Context context) throws IOException {
            int k = context.getConfiguration().getInt("n", 100);
            queue = new TopScoredObjects<>(k);
        }

        @Override
        public void map(Text nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {
            queue.add(node.getNodeId().toString(), node.getPageRank());
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();
            FloatWritable value = new FloatWritable();

            for (PairOfObjectFloat<String> pair : queue.extractAll()) {
                key.set(pair.getLeftElement());
                value.set(pair.getRightElement());
                context.write(key, value);
            }
        }
    }

    private static class MyReducer extends
            Reducer<Text, FloatWritable, Text, Text> {
        private static TopScoredObjects<String> queue;

        @Override
        public void setup(Context context) throws IOException {
            int k = context.getConfiguration().getInt("n", 100);
            queue = new TopScoredObjects<String>(k);
        }

        @Override
        public void reduce(Text nid, Iterable<FloatWritable> iterable, Context context)
                throws IOException {
            Iterator<FloatWritable> iter = iterable.iterator();
            queue.add(nid.toString(), iter.next().get());

            // Shouldn't happen. Throw an exception.
            if (iter.hasNext()) {
                throw new RuntimeException();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();
            Text value = new Text();

            for (PairOfObjectFloat<String> pair : queue.extractAll()) {
                key.set(pair.getLeftElement());
                // We're outputting a string so we can control the formatting.
                value.set(String.format("%.5f", pair.getRightElement()));
                context.write(key, value);
            }
        }
    }

    public FindMaxPageRankNodes() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(TOP));

        LOG.info("Tool name: " + FindMaxPageRankNodes.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + n);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("n", n);

        Job job = Job.getInstance(conf);
        job.setJobName(FindMaxPageRankNodes.class.getName() + ":" + inputPath);
        job.setJarByClass(FindMaxPageRankNodes.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Text instead of FloatWritable so we can control formatting

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

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
        int res = ToolRunner.run(new FindMaxPageRankNodes(), args);
        System.exit(res);
    }
}
