package knn.graph;

import java.io.Serializable;

public interface IMetric<TID, T, TN extends INode<TID, T>> extends Serializable, SimilarityInterface<T>
{
	/*
	 * this will calculate the similarity or the distance depending on what is necessary to make comparisons
	 */
	 public double compare(T a_, T b_);

	 public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_);

	 public NeighborListFactory<TID, T, TN> getNeighborListFactory();
}
