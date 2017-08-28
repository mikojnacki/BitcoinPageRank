package com.mikolaj.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * WEIGHTED VERSION - add txout.value (in_address value)
 *
 * Based on https://github.com/adamjshook/mapreducepatterns
 * Credits to Adam J. Shook @adamjshook
 *
 * Classes to be used from withing PrepareDataset.class runSecondJoin() method to perform query:
 *
 * SELECT txinprevid.tx_id, out1.address AS in_address, out1.value AS in_value
 * JOIN txout out1 ON (txinprevid.prev_id = out1.tx_id AND txinprevid.prev_out_index = out1.tx_idx)
 *
 * The txinprevid input dataset consist of columns:
 *
 * | prev_out | prev_out_index | tx_id | tx.id |
 *
 * The result is a table with given columns:
 *
 * | txinprevid.tx_id | out1.address (as in_address) | out1.values (as in_value) |
 *
 */
public class PrepareSecondJoin {
    private static final Logger LOG = Logger.getLogger(PrepareSecondJoin.class);

    public static class TxinprevidJoinMapper extends Mapper<Object, Text, PairOfStrings, Text> {

        private PairOfStrings outkey = new PairOfStrings();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string: | prev_out | prev_out_index | tx_id | tx.id |
            String[] txinprevidRecord = value.toString().trim().split(",");
            // txinprevidRecord[1] - 1rd column with name prev_out_index
            // txinprevidRecord[3] -  3rd column with name prev_id (before tx.id)

            if (txinprevidRecord[1] == null || txinprevidRecord[3] == null) {
                return;
            }

            // Set foreign key
            outkey.set(txinprevidRecord[1], txinprevidRecord[3]);
            //LOG.info("TxinprevidJoinMapper: " + outkey.toString() + " " + txinprevidRecord[2]);

            // Flag this record for the reducer and then output
            outvalue.set("A" + txinprevidRecord[2]);
            context.write(outkey, outvalue);
        }
    }

    public static class TxoutAsOut1JoinMapper extends Mapper<Object, Text, PairOfStrings, Text> {

        // txin JOIN txout ON txin.prev_out = tx.hash

        private PairOfStrings outkey = new PairOfStrings();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

//            id BIGINT
//            ,tx_idx INT
//            ,address VARCHAR(34)
//            ,value BIGINT
//            ,type VARCHAR(8)
//            ,tx_id BIGINT

            // Split the input string
            String[] txoutRecord = value.toString().trim().split(",");
            // txoutRecord[5] - 6th column with name txout.tx_id
            // txoutRecord[1] - 2nd column with name txout.tx_idx
            // txoutRecord[2] - 3rd column with name txout.address (as in_address)
            // txoutRecord[3] - 4th column with name txout.value (as in_value)

            if (txoutRecord[5] == null || txoutRecord[1] == null) {
                return;
            }

            // Set foreign key
            outkey.set(txoutRecord[1], txoutRecord[5]);
            //LOG.info("TxoutAsOut1JoinMapper: " + outkey.toString() + " " + txoutRecord[2]);

            // Flag this record for the reducer and then output
            outvalue.set("B" + txoutRecord[2] + "," + txoutRecord[3]);
            context.write(outkey, outvalue);
        }
    }

    public static class SecondJoinReducer extends Reducer<PairOfStrings, Text, Text, Text> {

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
        public void reduce(PairOfStrings key, Iterable<Text> values, Context context)
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
