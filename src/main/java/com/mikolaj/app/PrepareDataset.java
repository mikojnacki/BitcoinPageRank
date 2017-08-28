package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * VERSION FOR WEIGHTED PAGERANK
 *
 * Class to perform a query:
 *
 * SELECT out1.address as in_address, out2.address as out_address
 * FROM (SELECT txin.prev_out, txin.prev_out_index, txin.tx_id, tx.id AS prev_id FROM txin JOIN tx ON txin.prev_out = tx.hash) txinprevid
 * JOIN txout out1 ON (txinprevid.prev_id = out1.tx_id AND txinprevid.prev_out_index = out1.tx_idx)
 * JOIN txout out2 ON txinprevid.tx_id = out2.tx_id;
 *
 * The result is a table with 2 columns:
 * | in_address | out_address | amount |
 *
 * The result table is used by BuildTextGraph class to create adjacency list of graph of Bitcoin addresses
 */
public class PrepareDataset extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPageRankBasic.class);

    // Mappers and Reducers defined in:
    // - PrepareFirstJoin class
    // - PrepareSecondJoin class
    // - PrepareThirdJoin class

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        ToolRunner.run(new PrepareDataset(), args);
        System.out.println("\nAll jobs finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    public PrepareDataset() {}

    private static final String TX_INPUT = "tx";
    private static final String TXIN_INPUT = "txin";
    private static final String TXOUT_INPUT = "txout";
    private static final String OUTPUT = "output";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");

        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("tx input path").create(TX_INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("txin input path").create(TXIN_INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("txout input path").create(TXOUT_INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));


        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(TX_INPUT) || !cmdline.hasOption(TXIN_INPUT)
                || !cmdline.hasOption(TXOUT_INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String txInputPath = cmdline.getOptionValue(TX_INPUT);
        String txinInputPath = cmdline.getOptionValue(TXIN_INPUT);
        String txoutInputPath = cmdline.getOptionValue(TXOUT_INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        LOG.info("Tool name: " + PrepareDataset.class.getSimpleName());
        LOG.info(" - txInputDir: " + txInputPath);
        LOG.info(" - txinInputDir: " + txinInputPath);
        LOG.info(" - txoutInputDir: " + txoutInputPath);
        LOG.info(" - outputDir: " + outputPath);

        // Run JOIN jobs
        runFirstJoin(txinInputPath, txInputPath, outputPath, "inner");
        runSecondJoin(outputPath + "/first-join", txoutInputPath, outputPath, "inner");
        runThirdJoin(outputPath + "/second-join", txoutInputPath, outputPath, "inner");
        runFourthJoin(outputPath + "/third-join", txInputPath, outputPath, "inner");
        runMapToEdges(outputPath + "/fourth-join", outputPath);
        runDistinctOutAddresses(outputPath + "/edges", outputPath);
        runRemainingNodesJoin(outputPath + "/distinct-out-addresses",
                outputPath + "/edges", outputPath, "leftouter"); // left outer join with result of Third JOIN

        //TODO: consider to remove mid-steps data

        return 0;
    }

    private void runFirstJoin(String txinInputPath, String txInputPath, String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        // (SELECT txin.*, tx.id AS prev_id FROM txin JOIN tx ON txin.prev_out = tx.hash) txinprevid

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - first JOIN");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);


        String out = outputPath + "/first-join";

        LOG.info("First JOIN");
        LOG.info(" - txinInput: " + txinInputPath);
        LOG.info(" - txInput: " + txInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(txinInputPath),
                TextInputFormat.class, PrepareFirstJoin.TxinJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txInputPath),
                TextInputFormat.class, PrepareFirstJoin.TxJoinMapper.class);

        job.setReducerClass(PrepareFirstJoin.FirstJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("First JOIN job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    private void runSecondJoin(String firstJoinInputPath, String txoutInputPath, String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        //SELECT txinprevid.tx_id, out1.addres as in_address
        //JOIN txout out1 ON (txinprevid.prev_id = out1.tx_id AND txinprevid.prev_out_index = out1.tx_idx)

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - second JOIN");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);

        String out = outputPath + "/second-join";

        LOG.info("Second JOIN");
        LOG.info(" - firstJoinResultInput: " + firstJoinInputPath);
        LOG.info(" - txoutInput: " + txoutInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(firstJoinInputPath),
                TextInputFormat.class, PrepareSecondJoin.TxinprevidJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txoutInputPath),
                TextInputFormat.class, PrepareSecondJoin.TxoutAsOut1JoinMapper.class);

        job.setReducerClass(PrepareSecondJoin.SecondJoinReducer.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Second JOIN job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    }

    private void runThirdJoin(String secondJoinInputPath, String txoutInputPath, String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        //SELECT out1.addres as in_address, out2.address as out_addres
        //JOIN txout out2 ON txinprevid.tx_id = out2.tx_id;

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - third JOIN");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);

        String out = outputPath + "/third-join";

        LOG.info("Third JOIN");
        LOG.info(" - secondJoinResultInput: " + secondJoinInputPath);
        LOG.info(" - txoutInput: " + txoutInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(secondJoinInputPath),
                TextInputFormat.class, PrepareThirdJoin.TxinprevidJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txoutInputPath),
                TextInputFormat.class, PrepareThirdJoin.TxoutAsOut2JoinMapper.class);

        job.setReducerClass(PrepareThirdJoin.ThirdJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Third JOIN job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    }

    private void runFourthJoin(String thirdJoinInputPath, String txInputPath, String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        //SELECT in_address, in_value, out_addres, out_value FROM third-join
        //JOIN tx A ON third-join.tx_id = A.id;

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - fourth JOIN");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);

        String out = outputPath + "/fourth-join";

        LOG.info("Fourth JOIN");
        LOG.info(" - thirdJoinResultInput: " + thirdJoinInputPath);
        LOG.info(" - txInput: " + txInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(thirdJoinInputPath),
                TextInputFormat.class, PrepareFourthJoin.ThirdJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txInputPath),
                TextInputFormat.class, PrepareFourthJoin.TxJoinMapper.class);

        job.setReducerClass(PrepareFourthJoin.FourthJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Fourth JOIN job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    private void runMapToEdges(String fourthJoinInputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {
        // map result of fourth-join into proper edge list:
        // in_address, out_address, amount

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - map to edges");
        job.setJarByClass(PrepareDataset.class);

        String out = outputPath + "/edges";

        LOG.info("Mapping to edges");
        LOG.info(" - fourthJoinResultInput: " + fourthJoinInputPath);
        LOG.info(" - output: " + out);

        FileInputFormat.addInputPath(job, new Path(fourthJoinInputPath));

        job.setMapperClass(PrepareMapToEdges.EdgesMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Map to edges job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    private void runDistinctOutAddresses(String edgesInputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {

        // SELECT DISTINCT A.out_address FROM edges

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - Distinct out addresses");
        job.setJarByClass(PrepareDataset.class);

        String out = outputPath + "/distinct-out-addresses";

        LOG.info("DISTINCT out addresses");
        LOG.info(" - edgesInputPath: " + edgesInputPath);
        LOG.info(" - output: " + out);

        FileInputFormat.addInputPath(job, new Path(edgesInputPath));

        job.setMapperClass(PrepareDistinctOutAddresses.DistinctMapper.class);
        job.setReducerClass(PrepareDistinctOutAddresses.DistinctReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("DISTINCT job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    private void runRemainingNodesJoin(String distinctOutAddressesInputPath, String edgesInputPath,
                                       String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - remaining nodes JOIN");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);
        job.getConfiguration().set("mapreduce.output.basename", "remaining-nodes");

        // since here we get one column, we dont want a trailing comma
        job.getConfiguration().set("mapred.textoutputformat.separator", "");

        String out = outputPath + "/remaining-nodes-join";

        LOG.info("Remaining nodes JOIN");
        LOG.info(" - distinctOutAddressesInput: " + distinctOutAddressesInputPath);
        LOG.info(" - edgesInput: " + edgesInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(distinctOutAddressesInputPath),
                TextInputFormat.class, PrepareRemainingNodesJoin.DistinctOutAddressesJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(edgesInputPath),
                TextInputFormat.class, PrepareRemainingNodesJoin.OutAddressesJoinMapper.class);

        job.setReducerClass(PrepareRemainingNodesJoin.RemainingNodesJoinReducer.class);
        job.setNumReduceTasks(1); // not sure

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Remaining nodes JOIN job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}
