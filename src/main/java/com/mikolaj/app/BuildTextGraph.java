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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Mikolaj on 17.08.17.
 *
 * Job takes text data consisting of 2 columns, delimited by whitespace, returned by this generic HIVE query:
 *
 * SELECT out1.address as in_address, out2.address as out_address
 * FROM (SELECT txin.prev_out, txin.prev_out_index, txin.tx_id, tx.id AS prev_id FROM txin JOIN tx ON txin.prev_out = tx.hash ) txinprevid
 * JOIN txout out1 ON (txinprevid.prev_id = out1.tx_id AND txinprevid.prev_out_index = out1.tx_idx)
 * JOIN txout out2 ON txinprevid.tx_id = out2.tx_id
 * JOIN tx ON txinprevid.tx_id = tx.id;
 *
 *  Example of data: inAddress> outAddress
 *
 *  12VNkCDJadLMS7oDvVZXY9NrFEiihCvyA4 1BpkG9FwkQLbT7irsvwG7HUz3QgTdCsZYs
 *  1AH8157cSKaULn2dxZiGnYzYnvmBrTAAHq 1NLM9wmdMsUoeRmDdqjE5VjNXwot1MV68q
 *  ....
 *
 *  Job parses and groups it, giving on the output 2 columns: inAddress (node id) and list of outAddresses (adjacency list)
 *
 */
public class BuildTextGraph extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildPageRankRecords.class);

    private static final String UNKNOWN_ADRESS = "unknown";

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text inAddress = new Text();
        private static final Text outAddress = new Text();

        public void map(LongWritable key, Text t, Context context) throws IOException, InterruptedException {

            String[] arr = t.toString().trim().split("\\s+");

            if (arr.length != 2) {
                throw new RuntimeException("Wrong input data! Should be '<inAddress> <outAdress>'");
            } else {
                inAddress.set(arr[0]);
                outAddress.set(arr[1]);
            }

            // omit edges including unknown addresses
            if (!arr[0].equals(UNKNOWN_ADRESS) || !arr[1].equals(UNKNOWN_ADRESS)) {
                context.getCounter("graph", "numNodes").increment(1);
                context.getCounter("graph", "numEdges").increment(1);
                // emit
                context.write(inAddress, outAddress);
            }

        }

    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private static String DELIMITER = " "; // single whitespace

        @Override
        public void reduce(Text inAddress, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String outAddresses = "";
            // build String delimited with space and save it as Text
            for (Text value : values) {
                outAddresses = outAddresses + value.toString() + DELIMITER;
            }
            // emit
            context.write(inAddress, new Text(outAddresses));
        }

    }


    public BuildTextGraph() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        LOG.info("Tool name: " + BuildTextGraph.class.getSimpleName());
        LOG.info(" - inputDir: " + inputPath);
        LOG.info(" - outputDir: " + outputPath);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(BuildTextGraph.class.getSimpleName() + ":" + inputPath);
        job.setJarByClass(BuildTextGraph.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        ToolRunner.run(new BuildPageRankRecords(), args);
    }
}
