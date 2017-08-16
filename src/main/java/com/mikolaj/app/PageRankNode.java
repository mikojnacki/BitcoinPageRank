package com.mikolaj.app;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Mikolaj on 16.08.17.
 */
public class PageRankNode implements Writable {
//    public static enum Type {
//        Complete((byte) 0),  // PageRank mass and adjacency list.
//        Mass((byte) 1),      // PageRank mass only.
//        Structure((byte) 2); // Adjacency list only.
//
//        public byte val;
//
//        private Type(byte v) {
//            this.val = v;
//        }
//    };
//
//    private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

    private IntWritable type;
    private Text nodeid;
    private FloatWritable pagerank;
    private ArrayListWritable<Text> adjacenyList;

    public PageRankNode() {}

    public FloatWritable getPageRank() {
        return pagerank;
    }

    public void setPageRank(FloatWritable p) {
        this.pagerank = p;
    }

    public Text getNodeId() {
        return nodeid;
    }

    public void setNodeId(Text n) {
        this.nodeid = n;
    }

    public ArrayListWritable<Text> getAdjacenyList() {
        return adjacenyList;
    }

    public void setAdjacencyList(ArrayListWritable<Text> list) {
        this.adjacenyList = list;
    }

    public IntWritable getType() {
        return type;
    }

    public void setType(IntWritable type) {
        this.type = type;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     * @throws IOException if any exception is encountered during object deserialization
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        type.readFields(in);
        nodeid.readFields(in);

        if (type.equals(new IntWritable(1))) { // Mass
            pagerank.readFields(in);
            return;
        }

        if (type.equals(new IntWritable(0))) { // Complete
            pagerank.readFields(in);
        }

        adjacenyList = new ArrayListWritable<>();
        adjacenyList.readFields(in);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     * @throws IOException if any exception is encountered during object serialization
     */
    @Override
    public void write(DataOutput out) throws IOException {
        type.write(out);
        nodeid.write(out);

        if (type.equals(new IntWritable(1))) { // Mass
            pagerank.write(out);
            return;
        }

        if (type.equals(new IntWritable(0))) { // Complete
            pagerank.write(out);
        }

        adjacenyList.write(out);
    }
}
