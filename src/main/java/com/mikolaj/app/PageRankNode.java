package com.mikolaj.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Mikolaj on 16.08.17.
 */
public class PageRankNode implements Writable {
    public static enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    };

    private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

    private Type type;
    private Text nodeid;
    private float pagerank;
    private ArrayListWritable<Text> adjacenyList;

    public PageRankNode() {}

    public float getPageRank() {
        return pagerank;
    }

    public void setPageRank(float p) {
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

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
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
        int b = in.readByte();
        type = mapping[b];
        nodeid.readFields(in);

        if (type.equals(Type.Mass)) {
            pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
            pagerank = in.readFloat();
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
        out.writeByte(type.val);
        nodeid.write(out);

        if (type.equals(Type.Mass)) {
            out.writeFloat(pagerank);
            return;
        }

        if (type.equals(Type.Complete)) {
            out.writeFloat(pagerank);
        }

        adjacenyList.write(out);
    }
}
