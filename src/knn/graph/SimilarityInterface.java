package knn.graph;

import java.io.Serializable;

/**
 *
 * @author tibo
 * @param <t>
 */
public interface SimilarityInterface<t> extends Serializable 
{
    public double similarity(t value1, t value2);
}
