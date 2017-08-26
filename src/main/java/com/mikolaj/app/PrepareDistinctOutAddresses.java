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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runDistinctOutAddresses() method to perform query:
 *
 * SELECT DISTINCT A.out_address FROM edges
 *
 * Where edges is a result of 3 previous joins (runFirstJoin(), runSecondJoin(), runThirdJoin()) with table structure:
 * | in_address | out address |
 *
 * The result is a edges table with columns:
 * | out_address |
 *
 *
 */
public class PrepareDistinctOutAddresses extends Configured implements Tool {
    private static final Logger LOG = Logger .getLogger(PrepareFirstJoin.class);

    // SELECT DISTINCT A.out_address FROM edges

    public static class DistinctMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text outAddress = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] mapRecord = value.toString().trim().split(",");
            // out_address -> mapRecord[1]
            if (mapRecord[1] == null) {
                return;
            }
            outAddress.set(mapRecord[1]);
            context.write(outAddress, NullWritable.get());
        }
    }

    public static class DistinctReducer extends
            Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        public void reduce(Text outAddress, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(outAddress, NullWritable.get());
        }

    }

// ##########################################################################

    public PrepareDistinctOutAddresses() {}

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

        LOG.info("Tool name: " + PrepareDistinctOutAddresses.class.getSimpleName());
        LOG.info(" - inputDir: " + inputPath);
        LOG.info(" - outputDir: " + outputPath);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(PrepareDistinctOutAddresses.class.getSimpleName() + ":" + inputPath);
        job.setJarByClass(PrepareDistinctOutAddresses.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);

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
        ToolRunner.run(new PrepareDistinctOutAddresses(), args);
    }
}


// ##########################################################################
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//                .getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: UniqueUserCount <in> <out>");
//            System.exit(2);
//        }
//
//        Path tmpout = new Path(otherArgs[1] + "_tmp");
//        FileSystem.get(new Configuration()).delete(tmpout, true);
//        Path finalout = new Path(otherArgs[1]);
//        Job job = new Job(conf, "StackOverflow Unique User Count");
//        job.setJarByClass(UniqueUserCount.class);
//        job.setMapperClass(SODistinctUserMapper.class);
//        job.setCombinerClass(SODistinctUserReducer.class);
//        job.setReducerClass(SODistinctUserReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setNumReduceTasks(1);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, tmpout);
//
//        boolean exitCode = job.waitForCompletion(true);
//        if (exitCode) {
//            job = new Job(conf, "Stack Overflow Unique User Count");
//            job.setJarByClass(UniqueUserCount.class);
//            job.setMapperClass(SOUserCounterMapper.class);
//            job.setCombinerClass(IntSumReducer.class);
//            job.setReducerClass(IntSumReducer.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(IntWritable.class);
//            job.setInputFormatClass(SequenceFileInputFormat.class);
//            FileInputFormat.addInputPath(job, tmpout);
//            FileOutputFormat.setOutputPath(job, finalout);
//            exitCode = job.waitForCompletion(true);
//        }
//
//        System.exit(exitCode ? 0 : 1);
//    }
//}
