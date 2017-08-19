package com.mikolaj.app;

/**
 * Based on https://github.com/lintool/tools/blob/master/lintools-datatypes/src/main/java/tl/lin/data/pair/PairOfObjectFloat.java
 * Credits to Jimmy Lin @lintool
 */
public class PairOfObjectFloat<L extends Comparable<L>> implements Comparable<PairOfObjectFloat<L>> {

    private L left;
    private float right;

    public PairOfObjectFloat(L left, float right) {
        this.left = left;
        this.right = right;
    }

    public PairOfObjectFloat() {
    }

    public L getLeftElement() {
        return left;
    }

    public float getRightElement() {
        return right;
    }

    public void set(L left, float right) {
        this.left = left;
        this.right = right;
    }

    public void setLeftElement(L left) {
        this.left = left;
    }

    public void setRightElement(float right) {
        this.right = right;
    }

    /**
     * Generates human-readable String representation of this pair.
     */
    public String toString() {
        return "(" + left + ", " + right + ")";
    }

    /**
     * Creates a shallow clone of this object; the left element itself is not cloned.
     */
    public PairOfObjectFloat<L> clone() {
        return new PairOfObjectFloat<L>(left, right);
    }

    @Override
    public int compareTo(PairOfObjectFloat<L> that) {
        if (this.left.equals(that.left)) {
            return that.right > this.right ? -1 : 1;
        }
        return this.left.compareTo(that.left);
    }
}
