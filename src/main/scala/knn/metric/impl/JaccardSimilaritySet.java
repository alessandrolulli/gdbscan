package knn.metric.impl;

import java.util.Set;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListComparatorDESC;
import knn.graph.NeighborListFactory;
import knn.metric.IMetric;

public class JaccardSimilaritySet<TID, E, T extends Set<E>, TN extends INode<TID, T>> implements IMetric<TID, T, TN>
{
	private static final long serialVersionUID = 1L;

	@Override
	public double compare(T a_, T b_)
	{
		int intersection = 0;
		
		T small = a_;
		T large = b_;
		if(a_.size() > b_.size())
		{
			small = b_;
			large = a_;
		}
		
		for(E el : small)
		{
			if(large.contains(el))
			{
				intersection++;
			}
		}

	    return (double)intersection / (small.size() + large.size() - intersection);
	}

	@Override
	public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_)
	{
		return isValid(neighbor_.similarity, epsilon_);
	}

	public boolean isValid(double val_, double epsilon_)
	{
		return val_ >= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, T, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorDESC<TID, T, TN>());
	}

	@Override
	public double similarity(T a_, T b_)
	{
		return compare(a_, b_);
	}
}
