package knn.metric;

import java.io.Serializable;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListFactory;

public interface IMetric<TID, T, TN extends INode<TID, T>> extends Serializable
{
	/*
	 * this will calculate the similarity or the distance depending on what is necessary to make comparisons
	 */
	 public double compare(T a_, T b_);

	 public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_);

	 public NeighborListFactory<TID, T, TN> getNeighborListFactory();
	 
	 public double similarity(T a_, T b_);
}
