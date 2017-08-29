package com.mikolaj.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * WEIGHTED VERSION - add txout.value (out_address value) and add tx_id (for 4th JOIN purpose)
 *
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runThirdJoin() method to perform query:
 *
 * SELECT in_address, in_value, out_addres, out_value FROM third-join
 * JOIN tx A ON third-join.tx_id = A.id;
 *
 *  The third-join (after 3nd join) input dataset consist of columns:
 *
 * | tx_id | out1.address (as in_address) | ou1.value (as in_value) | out2.address (as out_address) | out2.value (as out_value) |
 *
 * The result is a table with given columns:
 *
 * | out1.address (as in_address) | ou1.value (as in_value) | out2.address (as out_address) | out2.value (as out_value) | tx.sum_value |
 */
public class PrepareFourthJoin {
    private static final Logger LOG = Logger.getLogger(PrepareSecondJoin.class);

    public static class ThirdJoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string:
            // | tx.id | in_address | in_value | out_address | out_value |
            String[] record = value.toString().trim().split(",");
            // record[0] - 1st column with name id
            // record[1] - 2nd column with name in_address
            // record[2] - 3rd column with name in_value
            // record[3] - 4th column with name out_address
            // record[4] - 5th column with name out_value

            if (record[0] == null) {
                return;
            }

            // Set foreign key
            outkey.set(record[0]);

            // Flag this record for the reducer and then output
            outvalue.set("A" + record[1] + "," + record[2] + "," + record[3] + "," + record[4]);
            //outvalue.set("A" + record[0] + "," + record[1] + "," + record[2] + "," + record[3] + "," + record[4]);
            context.write(outkey, outvalue);
        }
    }

    public static class TxJoinMapper extends Mapper<Object, Text, Text, Text> {
        // third-join JOIN tx A ON third-join.id = tx.id

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String[] txRecord = value.toString().trim().split(",");
            //txRecord[0] -> Tx 1st column "id"
            //txRecord[3] -> Tx 4th column "out_value"

            if (txRecord[0] == null) {
                return;
            }

            // The foreign join key is the user ID
            outkey.set(txRecord[0]);

            // Flag this record for the reducer and then output
            outvalue.set("B" + txRecord[3]);

            context.write(outkey, outvalue);
        }
    }

    public static class FourthJoinReducer extends Reducer<Text, Text, Text, Text> {
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
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output A by itself
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
}