package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runRemainingNodesJoin() method to perform query:
 *
 * SELECT A.out_address FROM distinc_out_addresses A
 * LEFT OUTER JOIN distinc_out_addresses B ON A.out_address = B.in_address WHERE B.in_address IS NULL;
 *
 * The result is a table with 1 column:
 *
 * | out_addresses |
 *
 * which stands for nodes (from out addresses) that should be added to adjacency list
 *
 */
public class PrepareRemainingNodesJoin extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PrepareRemainingNodesJoin.class);

    public static class DistinctOutAddressesJoinMapper extends Mapper<Object, Text, Text, Text> {

        // takes distinct out addresses from table which is a result of
        // SELECT DISTINCT A.out_address FROM edges from PrepareDistinctOutAddresses

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String record = value.toString().trim(); // only one column

            if (record == null) {
                return;
            }

            // The foreign join key
            outkey.set(record);

            // Flag this record for the reducer and then output
            outvalue.set("A" + record);
            context.write(outkey, outvalue);
        }
    }

    public static class OutAddressesJoinMapper extends Mapper<Object, Text, Text, Text> {

        // takes out addresses form table edges

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String[] record = value.toString().trim().split(",");
            String inAddress = record[0]; // 1st column stands for in address
            //txRecord[1] -> 2nd column stands for out address

            if (inAddress == null) {
                return;
            }

            // The foreign join key
            outkey.set(inAddress);

            // Flag this record for the reducer and then output
            outvalue.set("B" + inAddress);

            context.write(outkey, outvalue);
        }
    }

    public static class RemainingNodesJoinReducer extends Reducer<Text, Text, Text, Text> {

        // txin JOIN txout ON txin.prev_out = tx.hash

        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        @Override
        public void setup(Context context) {
            // Get the type of join from our configuration
            joinType = context.getConfiguration().get("join.type");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Clear our lists
            listA.clear();
            listB.clear();

            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            for (Text t : values) {
                if (t.charAt(0) == 'A') {
                    listA.add(new Text(t.toString().substring(1)));
                } else if (t.charAt(0) == 'B') {
                    listB.add(new Text(t.toString().substring(1)));
                }
            }

            // Execute our join logic now that the lists are filled
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException,
                InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                // If both lists are not empty, join A with B
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                // For each entry in A,
                for (Text A : listA) {
                    // If list B is not empty, join A and B
//                    if (!listB.isEmpty()) {
//                        for (Text B : listB) {
//                            context.write(A, B);
//                        }
//                    } else {
//                        // Else, output A by itself
//                        context.write(A, new Text(""));
//                    }
                    // MCh: Propably change if-else else clause above and put this if to obtain WHERE B.in_address = NULL
                    if (listB.isEmpty()) {
                        context.write(A, new Text(""));
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {
                // FOr each entry in B,
                for (Text B : listB) {
                    // If list A is not empty, join A and B
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output B by itself
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {
                // If list A is not empty
                if (!listA.isEmpty()) {
                    // For each entry in A
                    for (Text A : listA) {
                        // If list B is not empty, join A with B
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {
                            // Else, output A by itself
                            context.write(A, new Text(""));
                        }
                    }
                } else {
                    // If list A is empty, just output B
                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("anti")) {
                // If list A is empty and B is empty or vice versa
                if (listA.isEmpty() ^ listB.isEmpty()) {

                    // Iterate both A and B with null values
                    // The previous XOR check will make sure exactly one of
                    // these lists is empty and therefore won't have output
                    for (Text A : listA) {
                        context.write(A, new Text(""));
                    }

                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else {
                throw new RuntimeException(
                        "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
            }
        }
    }


    // ##########################################################################

    public PrepareRemainingNodesJoin() {}

    private static final String INPUT_A = "inputA";
    private static final String INPUT_B = "inputB";
    private static final String OUTPUT = "output";
    private static final String JOIN_TYPE = "joinType";


    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path A").create(INPUT_A));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path B").create(INPUT_B));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("join").hasArg().withDescription("join type").create(JOIN_TYPE));
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT_A) || !cmdline.hasOption(INPUT_B)
                || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(JOIN_TYPE)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputAPath = cmdline.getOptionValue(INPUT_A);
        String inputBPath = cmdline.getOptionValue(INPUT_B);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        String joinType = cmdline.getOptionValue(JOIN_TYPE);

        LOG.info("Tool name: " + PrepareRemainingNodesJoin.class.getSimpleName());
        LOG.info(" - inputADir: " + inputAPath);
        LOG.info(" - inputBDir: " + inputBPath);
        LOG.info(" - outputDir: " + outputPath);
        LOG.info(" - joinType: " + joinType);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);


        Job job = Job.getInstance(conf);
        job.getConfiguration().set("join.type", joinType);
        job.setJobName(PrepareRemainingNodesJoin.class.getSimpleName());
        job.setJarByClass(PrepareRemainingNodesJoin.class);

        MultipleInputs.addInputPath(job, new Path(inputAPath),
                TextInputFormat.class, DistinctOutAddressesJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputBPath),
                TextInputFormat.class, OutAddressesJoinMapper.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(RemainingNodesJoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

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
        ToolRunner.run(new PrepareRemainingNodesJoin(), args);
    }
}
