package info.debatty.java.stringsimilarity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

public class SparseBooleanVector implements Serializable {
    
    /** 
     * Indicates the positions that hold the value "true"
     */
    protected int[] keys;

    public SparseBooleanVector(int size) {
        keys = new int[size];
    }
    
    public SparseBooleanVector() {
        this(20);
    }
    
    public SparseBooleanVector(HashMap<Integer, Integer> hashmap) {
        this(hashmap.size());
        SortedSet<Integer> sorted_keys = new TreeSet<Integer>(hashmap.keySet());
        int size = 0;
        for (int key : sorted_keys) {
            keys[size] = key;
            size++;
        }
    }

    /**
     * 
     * @param array 
     */
    public SparseBooleanVector(boolean[] array) {
        
        int size = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i]) {
                size++;
            }
        }
        
        keys = new int[size];
        int j = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i]) {
                keys[j] = i;
                j++;
            }
        }
    }
    
    
    /**
     * Computes and return the Jaccard index with other SparseVector.
     * |A inter B| / |A union B|
     * It is actually computed as |A inter B| / (|A| +|B| - | A inter B|)
     * using a single loop over A and B
     * @param other
     * @return 
     */
    public double jaccard(SparseBooleanVector other) {
        int intersection = this.intersection(other);
        return (double) intersection / (this.size() + other.size() - intersection);
    }
    
    /**
     * 
     * @param other
     * @return 
     */
    public int union(SparseBooleanVector other) {
        return this.size() + other.size() - this.intersection(other);
    }
    
    /**
     * 
     * @param other
     * @return 
     */
    public int intersection(SparseBooleanVector other) {
        int agg = 0;
        int i = 0;
        int j = 0;
        while (i < this.keys.length  && j < other.keys.length) {
            int k1 = this.keys[i];
            int k2 = other.keys[j];

            if (k1 == k2) {
                agg++;
                i++;
                j++;

            } else if (k1 < k2) {
                i++;
                
            } else {
                j++;
            }
        }
        return agg;
    }
    
    @Override
    public String toString() {
        String r = "";
        for (int i = 0; i < size(); i++) {
            r += keys[i] + ":" + keys[i] + " ";
        }
        
        return r;
    }

    /**
     * Return the number of (non-zero) elements in this vector.
     * @return 
     */
    public int size() {
        return this.keys.length;
    }
}
