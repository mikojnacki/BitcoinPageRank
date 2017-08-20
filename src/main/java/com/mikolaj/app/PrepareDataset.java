package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
 * Created by Mikolaj on 20.08.17.
 */
public class PrepareDataset extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPageRankBasic.class);

//    // Mappers
//    private static class MapClass extends Mapper<Text, Text, Text, Text> {
//
//        @Override
//        public void map() {
//
//        }
//    }
//
//    // Reducers
//    private static class ReduceClass extends Reducer<Text, Text, Text, Text> {
//
//        @Override
//        public void reduce() {
//
//        }
//    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PrepareDataset(), args);
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

        query1(txinInputPath, txInputPath, outputPath, "inner");
        //query2();
        //query3();

        return 0;
    }

    private void query1(String txinInputPath, String txInputPath, String outputPath, String joinType)
            throws IOException, InterruptedException, ClassNotFoundException {

        // (SELECT txin.*, tx.id AS prev_id FROM txin JOIN tx ON txin.prev_out = tx.hash) txinprevid

        Job job = Job.getInstance(getConf());
        job.setJobName("PrepareDataset - Query 1");
        job.setJarByClass(PrepareDataset.class);
        job.getConfiguration().set("join.type", joinType);

        String out = outputPath + "/query1";

        LOG.info("Query 1");
        LOG.info(" - txinInput: " + txinInputPath);
        LOG.info(" - txInput: " + txInputPath);
        LOG.info(" - output: " + out);
        LOG.info(" - joinType: " + joinType);

        MultipleInputs.addInputPath(job, new Path(txinInputPath),
                TextInputFormat.class, GenericReduceSideJoin.TxinJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txInputPath),
                TextInputFormat.class, GenericReduceSideJoin.TxJoinMapper.class);

        job.setReducerClass(GenericReduceSideJoin.TxinTxJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}
