package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * WEIGHTED VERSION
 *
 * Job takes text data consisting of 3 columns, delimited by comma, produced by PrepareDataset (edges)
 *
 *  Example of data:
 *
 *  12VNkCDJadLMS7oDvVZXY9NrFEiihCvyA4,1BpkG9FwkQLbT7irsvwG7HUz3QgTdCsZYs|1000
 *  1AH8157cSKaULn2dxZiGnYzYnvmBrTAAHq,1NLM9wmdMsUoeRmDdqjE5VjNXwot1MV68q|295341
 *  ....
 *
 *  Job parses and groups it, giving on the output 2 columns: inAddress (node id) and list of outAddress|amountSum (adjacency list)
 *  Modified -> creates graph insead of multigraph (reduces duplicate edges)
 *  Modified -> sum amounts
 *
 *  Seems OK
 */
public class BuildTextGraph extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildPageRankRecords.class);

    private static final String UNKNOWN_ADDRESS = "unknown";

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        //private static final Text inAddress = new Text();
        //private static final Text outAddressAndAmount = new Text();

        public void map(LongWritable key, Text t, Context context) throws IOException, InterruptedException {

            Text inAddress = new Text();
            Text outAddressAndAmount = new Text();

            String[] arr = t.toString().trim().split(",");
            String[] arrOut = arr[1].trim().split("\\|");

            if (arr.length != 2) {
                throw new RuntimeException("Wrong input data! Should be 'inAddress,outAddress|amount'");
            } else {
                inAddress.set(arr[0]);
                outAddressAndAmount.set(arr[1]);
            }

            // omit edges including unknown addresses
            if (!arr[0].equals(UNKNOWN_ADDRESS) && !arrOut[0].equals(UNKNOWN_ADDRESS)) {
                context.getCounter("graph", "numNodes").increment(1);
                context.getCounter("graph", "numEdges").increment(1);
                // emit
                context.write(inAddress, outAddressAndAmount);
            }

        }

    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private static String DELIMITER = " "; // single whitespace

        @Override
        public void reduce(Text inAddress, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Build HashMap from values, summing per key
            HashMap<String, Long> hm = new HashMap<>();
            String hmKey;
            String hmValue;
            // HashMap ensures deduplication. Sum amounts per key:
            for (Text t : values) {
                hmKey = t.toString().trim().split("\\|")[0];
                hmValue = t.toString().trim().split("\\|")[1];
                if (hm.containsKey(hmKey)) {
                    hm.put(hmKey, Long.valueOf(hmValue) + hm.get(hmKey));
                } else {
                    hm.put(hmKey, Long.valueOf(hmValue));
                }
            }

            // fold HashMap as one line String (adjcacency list) delimited with space and save it as Text

            // *** FIRST VERSION ***

//            // firstly count total outSum
//            BigDecimal outSum = new BigDecimal(0);
//            for (Long v : hm.values()) {
//                outSum = outSum.add(new BigDecimal(v));
//            }
//            // now build outAddressesAndWeights
//            String outAddressesAndWeights = "";
//            for (Map.Entry<String, Long> entry : hm.entrySet()) {
//                try {
//                    outAddressesAndWeights = outAddressesAndWeights + DELIMITER + entry.getKey() + "|"
//                            + String.valueOf(new BigDecimal(entry.getValue()).setScale(4, RoundingMode.HALF_EVEN).divide(outSum, RoundingMode.HALF_EVEN));
//                } catch (ArithmeticException e) {
//                    e.printStackTrace(); // may throw divide by zero exception?
//                }
//            }
//            context.write(inAddress, new Text(outAddressesAndWeights));

            // *** SECOND VERSION ***

            String outAddressesAndAmounts = "";
            Long outSum = 0L;
            for (Map.Entry<String, Long> entry : hm.entrySet()) {
                outAddressesAndAmounts = outAddressesAndAmounts + DELIMITER + entry.getKey() + "|"
                        + String.valueOf(entry.getValue());
                outSum = outSum + entry.getValue();
            }
            context.write(new Text(inAddress.toString() + "|" + String.valueOf(outSum)),
                    new Text(outAddressesAndAmounts));

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

        // *** Finalize creation of graph - add remaining nodes form out_address ***
        // *** Remaining nodes obtained by PrepareDataset metods: runDistinctOutAddresses() and runRemainingNodesJoin()
        // *** running equivalently to this SQL statement:
        // ***     SELECT A.out_address FROM (SELECT DISTINCT out_address FROM edges)
        // ***     A LEFT OUTER JOIN edges B ON A.out_address = B.in_address WHERE B.in_address IS NULL;

        // copy join-Output/remaining-nodes-join/part-r-00000 to graph-TextRecords
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("join-Output-Weighted/remaining-nodes-join/remaining-nodes-r-00000");
        Path dst = new Path(outputPath + "/");
        FileUtil.copy(fs, src, fs, dst, false, false, conf);
        // concat with remaining nodes
        Path srcDir = new Path(outputPath + "/");
        Path dstFile = new Path(outputPath + "/adjacency-list");
        FileUtil.copyMerge(fs, srcDir, fs, dstFile, false, conf, "");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildTextGraph(), args);

    }
}
