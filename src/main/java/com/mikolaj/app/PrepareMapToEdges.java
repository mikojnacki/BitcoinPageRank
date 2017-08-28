package com.mikolaj.app;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * ONLY IN WEIGHTED VERSION
 *
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runRunToEdges() method, takin input:
 *
 * | in_address | in_value | out_addres | out_value | sum_value |
 *
 * giving a result:
 *
 * | in_address | out_addresses | amount |
 *
 * where amount is calculated as: in_value * (out_value / sum_value)
 */
public class PrepareMapToEdges {
    private static final Logger LOG = Logger.getLogger(PrepareFirstJoin.class);

    public static class EdgesMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Split the input string:
            // | tx.id | in_address | in_value | out_address | out_value |
            String[] record = value.toString().trim().split(",");
            // record[0] - 1nd column with name in_address
            // record[1] - 2rd column with name in_value
            // record[2] - 3th column with name out_address
            // record[3] - 4th column with name out_value
            // record[4] - 5st column with name sum_value

            // Calculate amount, take care of zero values of sum_value
            BigDecimal amount;
            try {
                amount = (new BigDecimal(record[1]).multiply(new BigDecimal(record[3]))).divide(new BigDecimal(record[4]), RoundingMode.HALF_EVEN);
            } catch (ArithmeticException e) {
                //e.printStackTrace();
                amount = new BigDecimal("0");
            }

            // Set outvalue
            outvalue.set(record[0] + "," + record[2] + "|" + String.valueOf(amount));

            context.write(NullWritable.get(), outvalue);
        }
    }
}
