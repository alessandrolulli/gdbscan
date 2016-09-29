package knn.metric.impl;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListComparatorASC;
import knn.graph.NeighborListFactory;
import knn.metric.IMetric;
import knn.util.PointND;

public class EuclidianDistanceND<TID, T extends PointND, TN extends INode<TID, T>> implements IMetric<TID, T, TN>
{
	private static final long serialVersionUID = 1L;

	@Override
	public double compare(T a_, T b_)
	{
		double sum = 0;
		for(int i = 0 ; i < a_.size() ; i++)
		{
			sum += Math.pow(a_.get(i) - b_.get(i), 2);
		}
		
		return Math.sqrt(sum);
	}

	@Override
	public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_)
	{
		return isValid(neighbor_.similarity, epsilon_);
	}
	
	public boolean isValid(double val_, double epsilon_) 
	{
		return val_ <= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, T, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorASC<TID, T, TN>());
	}

	@Override
	public double similarity(T a_, T b_)
	{
		return 1 - compare(a_, b_);
	}
}
