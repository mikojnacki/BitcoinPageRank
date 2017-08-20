package com.mikolaj.app;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runFirstJoin() method to perform query:
 *
 * (SELECT txin.*, tx.id AS prev_id FROM txin JOIN tx ON txin.prev_out = tx.hash) txinprevid
 *
 * The result is a txinprevid table with columns:
 *
 * | prev_out | prev_out_index | tx_id | tx.id |
 *
 */
public class PrepareFirstJoin {
    private static final Logger LOG = Logger.getLogger(PrepareFirstJoin.class);

    public static class TxinJoinMapper extends Mapper<Object, Text, Text, Text> {

        // txin JOIN txout ON txin.prev_out = tx.hash

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String[] txinRecord = value.toString().trim().split(",");
            String txinPrevOut = txinRecord[2]; // Txin 3rd column with name prev_out

            if (txinPrevOut == null) {
                return;
            }

            // The foreign join key is the txinPrevOut
            outkey.set(txinPrevOut);

            // Flag this record for the reducer and then output
            outvalue.set("A" + txinRecord[2] + "," + txinRecord[3] + "," + txinRecord[4]);
            context.write(outkey, outvalue);
        }
    }

    public static class TxJoinMapper extends Mapper<Object, Text, Text, Text> {

        // txin JOIN txout ON txin.prev_out = tx.hash

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String[] txRecord = value.toString().trim().split(",");
            String txHash = txRecord[1]; // Tx 2rd column "hash"
            //txRecord[0] -> Tx 1st column "id"

            if (txHash == null) {
                return;
            }

            // The foreign join key is the user ID
            outkey.set(txHash);

            // Flag this record for the reducer and then output
            outvalue.set("B" + txRecord[0]);

            context.write(outkey, outvalue);
        }
    }

    public static class FirstJoinReducer extends Reducer<Text, Text, Text, Text> {

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
